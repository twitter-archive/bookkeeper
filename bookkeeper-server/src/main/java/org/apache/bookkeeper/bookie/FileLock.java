/**
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
 */
package org.apache.bookkeeper.bookie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

class FileLock {

    static final Logger LOG = LoggerFactory.getLogger(FileLock.class);

    static final String NAME = ".bookielock";

    final File lockFile;
    FileChannel fc = null;
    java.nio.channels.FileLock lock = null;

    FileLock(File dir) {
        this.lockFile = new File(dir, NAME);
    }

    synchronized void lock() throws IOException {
        if (!this.lockFile.exists()) {
            if (!this.lockFile.createNewFile()) {
                LOG.error("Lock file {}, that shouldn't exist, already exists. Another process is running ?");
                throw new IOException("Lock file " + this.lockFile
                        + " suddenly appeared, is another bookie process running?");
            }
        }
        this.fc = new RandomAccessFile(this.lockFile, "rw").getChannel();
        this.lock = this.fc.tryLock();
        if (null == this.lock) {
            throw new IOException("Failed on locking file " + this.lockFile);
        }
    }

    synchronized void release() {
        if (null != this.lock) {
            try {
                this.lock.release();
            } catch (IOException ioe) {
                LOG.error("Failed to unlock file {} : ", this.lockFile, ioe);
            }
        }
        if (null != this.fc) {
            try {
                this.fc.close();
            } catch (IOException e) {
                LOG.error("Failed to close file {} : ", this.lockFile, e);
            }
        }
    }
}
