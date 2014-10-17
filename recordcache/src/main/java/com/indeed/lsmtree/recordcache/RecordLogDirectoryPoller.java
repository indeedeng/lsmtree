/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.lsmtree.recordcache;

import com.indeed.lsmtree.recordlog.GenericRecordLogDirectoryPoller;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;
import com.indeed.util.io.checkpointer.Checkpointer;
import com.indeed.util.io.checkpointer.FileBasedCheckpointer;
import com.indeed.util.serialization.LongStringifier;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class RecordLogDirectoryPoller extends GenericRecordLogDirectoryPoller<Operation> {
    private static final Logger log = Logger.getLogger(RecordLogDirectoryPoller.class);

    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final File lastPositionFile) throws IOException {
        this(recordLogDirectory, fileCheckpointer(lastPositionFile));
    }

    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final File lastPositionFile, final boolean loop) throws IOException {
        this(recordLogDirectory, fileCheckpointer(lastPositionFile), loop);
    }

    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final File lastPositionFile, final boolean loop, final boolean deleteRecordLogs) throws IOException {
        this(recordLogDirectory, fileCheckpointer(lastPositionFile), loop, deleteRecordLogs);
    }

    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final Checkpointer<Long> checkpointer) throws IOException {
        super(recordLogDirectory, checkpointer);
    }

    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final Checkpointer<Long> checkpointer, final boolean loop) throws IOException {
        super(recordLogDirectory, checkpointer, loop);
    }

    /**
     * @param recordLogDirectory    record log directory
     * @param checkpointer          checkpointer
     * @param loop                  If true, poller will continually poll for new record logs. If false only polls once.
     * @param deleteRecordLogs      If true, poller will delete record logs up to but excluding the most recently read one.
     * @throws IOException
     */
    public RecordLogDirectoryPoller(final RecordLogDirectory<Operation> recordLogDirectory, final Checkpointer<Long> checkpointer, final boolean loop, final boolean deleteRecordLogs) throws IOException {
        super(recordLogDirectory, checkpointer, loop, deleteRecordLogs);
    }

    /**
     * Callback interface for processing record logs.
     */
    public interface Functions extends GenericRecordLogDirectoryPoller.Functions<Operation> {
        /**
         * Called once for each operation in a record log.
         *
         * @param position
         * @param op
         * @throws IOException
         */
        void process(long position, Operation op) throws IOException;
    }

    private static FileBasedCheckpointer<Long> fileCheckpointer(@Nonnull final File file) throws IOException {
        return new FileBasedCheckpointer<Long>(file, new LongStringifier(), 0L);
    }
}
