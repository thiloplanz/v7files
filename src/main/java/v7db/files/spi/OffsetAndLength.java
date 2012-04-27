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

/**
 * Support for "offset" and "length" parameters that can be used to create new
 * Content by taking a segment of other Content, with optional repetition.
 * 
 */

public final class OffsetAndLength implements Content {

	private final Content original;

	private final long offset, length;

	public OffsetAndLength(Content original, long offset, long length) {
		this.original = original;
		this.offset = offset;
		this.length = length;
	}

	public InputStream getInputStream() throws IOException {
		return getInputStream(0, length);
	}

	public InputStream getInputStream(long offset, long length)
			throws IOException {
		offset += this.offset;
		long oLen = original.getLength();
		if (length + offset <= oLen)
			return original.getInputStream(offset, length);

		return new RepeatedInputStream(offset, length);
	}

	class RepeatedInputStream extends InputStream {

		private long remaining;

		private InputStream chunk;

		private final long oLength;

		private long ooff;

		RepeatedInputStream(long offset, long length) {
			oLength = original.getLength();
			remaining = length;
			ooff = offset % oLength;
		}

		@Override
		public int read() throws IOException {
			if (remaining > 0) {
				if (chunk == null) {
					chunk = original.getInputStream(ooff, oLength - ooff);
					ooff = 0;
				}

				int x = chunk.read();
				if (x == -1) {
					chunk.close();
					chunk = null;
					return read();
				}
				remaining--;
				return x;
			}
			return -1;
		}

	}

	public long getLength() {
		return length;
	}

}
