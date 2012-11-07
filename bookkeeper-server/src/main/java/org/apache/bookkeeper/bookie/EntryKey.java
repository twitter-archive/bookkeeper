/**
 * Copyright The Apache Software Foundation
 *
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

import java.util.Comparator;

import com.google.common.primitives.Longs;

public class EntryKey {
    long ledgerId;
    long entryId;

    public EntryKey() {
        this(0, 0);
    }

    public EntryKey(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    /**
    * Comparator for the key portion
    */
    public static KeyComparator COMPARATOR = new KeyComparator();

    // Only compares the key portion
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EntryKey)) {
          return false;
        }
        EntryKey key = (EntryKey)other;
        return Longs.compare(ledgerId, key.ledgerId) == 0 &&
            Longs.compare(entryId, key.entryId) == 0;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(ledgerId) * 13 ^ Longs.hashCode(entryId) * 17;
    }
}

/**
* Compare EntryKey.
*/
class KeyComparator implements Comparator<EntryKey> {
    @Override
    public int compare(EntryKey left, EntryKey right) {
        int ret = Longs.compare(left.ledgerId, right.ledgerId);
        if (ret == 0)
            ret = Longs.compare(left.entryId, right.entryId);
        return ret;
    }
}
