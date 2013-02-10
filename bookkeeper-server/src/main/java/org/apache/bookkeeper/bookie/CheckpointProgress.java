/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.IOException;

/**
 * Interface to communicate checkpoint progress.
 */
public interface CheckpointProgress {

    /**
     * A checkpoint presented a time point. All entries added before this checkpoint are already persisted.
     */
    public static interface CheckPoint extends Comparable<CheckPoint> {

        public static final CheckPoint MAX = new CheckPoint() {

            @Override
            public int compareTo(CheckPoint o) {
                if (o == MAX) {
                    return 0;
                }
                return 1;
            }

            @Override
            public void checkpointComplete(boolean compact) throws IOException {
                // do nothing
            }

            @Override
            public boolean equals(Object o) {
                return this == o;
            }
            
        };
        /**
         * Tell checkpoint progress that the checkpoint is completed.
         * If <code>compact</code> is true, the implementation could compact
         * to reduce size of data containing old checkpoints.
         *
         * @param compact
         *          Flag to compact old checkpoints.
         */
        public void checkpointComplete(boolean compact) throws IOException;
    }

    /**
     * Request a checkpoint.
     * @return checkpoint.
     */
    public CheckPoint requestCheckpoint();

    /**
     * Start checkpointing for the given <i>checkpoint</i>
     *
     * @param checkpoint
     *          Check point.
     */
    public void startCheckpoint(CheckPoint checkpoint);
}
