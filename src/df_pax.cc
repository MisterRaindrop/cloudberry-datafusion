/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * df_pax.cc
 *	  Experimental: read PAX micro-partitions column by column on DataFusion's
 *	  worker threads, bypassing the table AM's row-at-a-time interface.
 *
 * Built as its own library, datafusion_pax.so, which the extension loads
 * with dlopen only when a PAX table is scanned; by then PostgreSQL has
 * loaded pax.so (RTLD_GLOBAL), and the PAX symbols used here bind to it.
 * It calls PAX's internal C++ classes, not a published API, so it must be
 * built against the headers of the very pax.so it runs with: the build
 * records pax.so's ELF build ID, and the extension refuses the library when
 * the running pax.so has another one.
 *
 * Threading contract:
 *   - df_pax_scan_begin runs on the backend's main thread: listing the
 *     micro-partitions visible to the snapshot reads PAX's auxiliary table.
 *     It copies what the workers need and keeps no PostgreSQL object.
 *   - df_pax_read_block may run on any thread, concurrently for different
 *     blocks.  It opens the block's files through PAX's LocalFileSystem
 *     (POSIX I/O), decodes the projected columns with PAX's reader (malloc,
 *     C++ exceptions, no PostgreSQL calls) and hands out rows group by group.
 *   - df_pax_scan_end frees the scan from any thread, once no reader runs.
 * No exception crosses these functions: failures come back as -1 and text.
 *
 * src/df_pax.cc
 *
 *-------------------------------------------------------------------------
 */
#include "comm/cbdb_api.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "comm/bitmap.h"
#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_columns.h"
#include "storage/filter/pax_filter.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/micro_partition_iterator.h"
#include "storage/micro_partition_metadata.h"
#include "storage/paxc_define.h"

#ifndef DF_PAX_BUILD_ID
#define DF_PAX_BUILD_ID ""
#endif

namespace {

struct DfBlock {
  std::string file;
  std::string visimap;
  bool toast;
};

struct DfScan {
  std::vector<DfBlock> blocks;
  int natts;
  std::vector<int> cols;     // 0-based table columns to hand out
  std::vector<int> widths;   // their value widths in bytes
  int read_col;              // column decoded when none is handed out
};

void SetError(char *err, size_t errlen, const char *what, const char *msg) {
  if (err && errlen > 0) snprintf(err, errlen, "%s: %s", what, msg);
}

}  // namespace

/* Rows of one group: values packed at 'width' bytes, one null byte each. */
typedef struct DfPaxColumn {
  const void *values;
  const uint8_t *nulls;
} DfPaxColumn;

typedef int (*DfPaxEmit)(void *ctx, uint32_t nrows, const DfPaxColumn *cols);

extern "C" {

__attribute__((visibility("default"))) const char *df_pax_shim_build_id(void) {
  return DF_PAX_BUILD_ID;
}

/*
 * List the micro-partitions of 'rel' visible to 'snapshot'.  'cols' are the
 * 0-based table columns to read, with their value widths.
 */
__attribute__((visibility("default"))) void *df_pax_scan_begin(
    Relation rel, Snapshot snapshot, const int *cols, const int *widths,
    int ncols, char *err, size_t errlen) {
  try {
    auto scan = std::make_unique<DfScan>();
    scan->natts = RelationGetNumberOfAttributes(rel);
    scan->cols.assign(cols, cols + ncols);
    scan->widths.assign(widths, widths + ncols);
    scan->read_col = ncols > 0 ? cols[0] : 0;

    auto it = pax::MicroPartitionIterator::New(rel, snapshot);
    while (it->HasNext()) {
      auto meta = it->Next();
      scan->blocks.push_back(DfBlock{meta.GetFileName(),
                                   meta.GetVisibilityBitmapFile(),
                                   meta.GetExistToast()});
    }
    it->Release();
    return scan.release();
  } catch (cbdb::CException &e) {
    SetError(err, errlen, "cannot list PAX micro-partitions", e.What().c_str());
  } catch (std::exception &e) {
    SetError(err, errlen, "cannot list PAX micro-partitions", e.what());
  } catch (...) {
    SetError(err, errlen, "cannot list PAX micro-partitions", "unknown error");
  }
  return nullptr;
}

__attribute__((visibility("default"))) int df_pax_scan_nblocks(void *scan) {
  return static_cast<int>(static_cast<DfScan *>(scan)->blocks.size());
}

__attribute__((visibility("default"))) void df_pax_scan_end(void *scan) {
  delete static_cast<DfScan *>(scan);
}

/*
 * Decode block 'index' and call 'emit' once per group with its visible rows.
 * Returns 0, or -1 with a message (also when 'emit' returns non-zero).
 */
__attribute__((visibility("default"))) int df_pax_read_block(
    void *scan_arg, int index, DfPaxEmit emit, void *ctx, char *err,
    size_t errlen) {
  auto scan = static_cast<DfScan *>(scan_arg);
  try {
    const DfBlock &block = scan->blocks.at(index);
    auto fs = pax::Singleton<pax::LocalFileSystem>::GetInstance();

    pax::MicroPartitionReader::ReaderOptions options;
    std::vector<bool> proj(scan->natts, false);
    for (int c : scan->cols) proj[c] = true;
    proj[scan->read_col] = true;
    options.filter = std::make_shared<pax::PaxFilter>();
    options.filter->SetColumnProjection(std::move(proj));

    std::shared_ptr<pax::Bitmap8> visimap;
    if (!block.visimap.empty()) {
      auto file = fs->Open(block.visimap, pax::fs::kReadMode);
      auto len = file->FileLength();
      visimap = std::make_shared<pax::Bitmap8>(len * 8);
      file->ReadN(visimap->Raw().bitmap, len);
      file->Close();
    }

    std::unique_ptr<pax::File> toast;
    if (block.toast)
      toast = fs->Open(block.file + TOAST_FILE_SUFFIX, pax::fs::kReadMode);
    auto reader = pax::MicroPartitionFileFactory::CreateMicroPartitionReader(
        options, pax::FLAGS_EMPTY, fs->Open(block.file, pax::fs::kReadMode),
        std::move(toast));

    const size_t ncols = scan->cols.size();
    std::vector<std::vector<char>> values(ncols);
    std::vector<std::vector<uint8_t>> nulls(ncols);
    std::vector<DfPaxColumn> out(ncols);

    for (size_t g = 0; g < reader->GetGroupNums(); g++) {
      auto group = reader->ReadGroup(g);
      auto &columns = *group->GetAllColumns();
      const size_t rows = group->GetRows();
      const size_t offset = group->GetRowOffset();

      // Rows deleted in the visibility map are left out.
      std::vector<uint32_t> keep;
      keep.reserve(rows);
      for (size_t r = 0; r < rows; r++)
        if (!visimap || !visimap->Test(static_cast<uint32>(offset + r)))
          keep.push_back(static_cast<uint32_t>(r));

      for (size_t k = 0; k < ncols; k++) {
        pax::PaxColumn *col = columns[scan->cols[k]].get();
        if (col == nullptr) {
          SetError(err, errlen, "PAX read", "projected column missing");
          return -1;
        }
        const int w = scan->widths[k];
        const char *buf = col->GetBuffer().first;
        const auto &bm = col->GetBitmap();  // bit set: the row has a value
        const bool dense = col->GetStorageFormat() !=
                           pax::PaxStorageFormat::kTypeStoragePorcVec;
        // The vectorized format keeps booleans bit-packed in memory, one
        // bit per row, least significant bit first; the plain format has
        // already decoded them to one byte per value.
        const bool bits = col->GetPaxColumnTypeInMem() ==
                          pax::PaxColumnTypeInMem::kTypeVecBitPacked;
        values[k].assign(keep.size() * w, 0);
        nulls[k].assign(keep.size(), 0);

        // In the plain format only non-null values are stored, in row
        // order; in the vectorized format every row has a slot.
        size_t nonnull = 0, next = 0;
        for (size_t r = 0; r < rows && next < keep.size(); r++) {
          const bool present = !bm || bm->Test(static_cast<uint32>(r));
          if (keep[next] == r) {
            if (present && bits) {
              const size_t i = dense ? nonnull : r;
              values[k][next] =
                  (static_cast<unsigned char>(buf[i >> 3]) >> (i & 7)) & 1;
            } else if (present) {
              const char *src = buf + (dense ? nonnull : r) * w;
              std::memcpy(values[k].data() + next * w, src, w);
            } else {
              nulls[k][next] = 1;
            }
            next++;
          }
          if (present) nonnull++;
        }
        out[k].values = values[k].data();
        out[k].nulls = nulls[k].data();
      }

      if (!keep.empty() &&
          emit(ctx, static_cast<uint32_t>(keep.size()), out.data()) != 0) {
        SetError(err, errlen, "PAX read", "the consumer stopped");
        return -1;
      }
    }
    reader->Close();
    return 0;
  } catch (cbdb::CException &e) {
    SetError(err, errlen, "cannot read PAX micro-partition", e.What().c_str());
  } catch (std::exception &e) {
    SetError(err, errlen, "cannot read PAX micro-partition", e.what());
  } catch (...) {
    SetError(err, errlen, "cannot read PAX micro-partition", "unknown error");
  }
  return -1;
}

}  // extern "C"
