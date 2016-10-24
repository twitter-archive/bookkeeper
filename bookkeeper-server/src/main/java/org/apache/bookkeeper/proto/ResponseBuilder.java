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
package org.apache.bookkeeper.proto;

import org.jboss.netty.buffer.ChannelBuffers;

import java.nio.ByteBuffer;

class ResponseBuilder {
    static BookieProtocol.Response buildErrorResponse(int errorCode, BookieProtocol.Request r) {
        return new BookieProtocol.ErrorResponse(r.getProtocolVersion(), r.getOpCode(),
                                                errorCode, r.getLedgerId(), r.getEntryId());
    }

    static BookieProtocol.Response buildAddResponse(BookieProtocol.Request r) {
        return new BookieProtocol.AddResponse(r.getProtocolVersion(), BookieProtocol.EOK, r.getLedgerId(),
                                              r.getEntryId());
    }

    static BookieProtocol.Response buildReadResponse(ByteBuffer data, BookieProtocol.Request r) {
        return new BookieProtocol.ReadResponse(r.getProtocolVersion(), BookieProtocol.EOK,
                                               r.getLedgerId(), r.getEntryId(), ChannelBuffers.wrappedBuffer(data));
    }
}
