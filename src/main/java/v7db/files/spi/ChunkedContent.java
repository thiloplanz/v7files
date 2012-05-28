/**
 * Copyright (c) 2012, Thilo Planz. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package v7db.files.spi;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.ArrayUtils;

public final class ChunkedContent implements Content {

	private final Content[] chunks;

	ChunkedContent(Content... chunks) {
		this.chunks = ArrayUtils.clone(chunks);
	}

	public InputStream getInputStream() throws IOException {
		return getInputStream(0, getLength());
	}

	public InputStream getInputStream(long offset, long length)
			throws IOException {
		return new ChunkStream(offset, length);
	}

	public long getLength() {
		int length = 0;
		for (Content c : chunks) {
			length += c.getLength();
		}
		return length;
	}

	class ChunkStream extends InputStream {

		private long remaining;

		private int currentChunk;

		private long currentOffset;

		private final long length;

		private InputStream currentStream;

		ChunkStream(long offset, long length) {
			this.length = getLength();
			if (offset < 0 && offset >= this.length)
				throw new IndexOutOfBoundsException("offset " + offset
						+ " is out of bounds");
			if (offset + length > this.length)
				throw new IndexOutOfBoundsException("length " + length
						+ " is out of bounds (offset: " + offset + ")");

			remaining = length;
			currentChunk = 0;
			currentOffset = 0;
			while (offset > 0) {
				Content chunk = chunks[currentChunk];
				if (chunk.getLength() <= offset) {
					currentChunk++;
					offset -= chunk.getLength();
				} else {
					currentOffset = offset;
					offset = 0;
				}
			}
		}

		@Override
		public int read() throws IOException {
			if (remaining <= 0)
				return -1;
			int r = -1;
			if (currentStream != null)
				r = currentStream.read();
			if (r > -1) {
				remaining--;
				return r;
			}
			if (currentStream != null) {
				currentStream.close();
				currentStream = null;
			}

			Content chunk = chunks[currentChunk];
			long len = chunk.getLength();
			if (len >= remaining + currentOffset) {
				currentStream = chunk.getInputStream(currentOffset, remaining);
			} else {
				currentStream = chunk.getInputStream(currentOffset, len
						- currentOffset);
			}
			currentOffset = 0;
			currentChunk++;
			return read();
		}

		@Override
		public void close() throws IOException {
			if (currentStream != null) {
				currentStream.close();
				currentStream = null;
			}
			remaining = 0;
			super.close();
		}

	}

}
