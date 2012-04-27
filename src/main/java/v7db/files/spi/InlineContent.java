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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import v7db.files.MapUtils;

public final class InlineContent implements Content, ContentPointer {

	private final long length;

	private final byte[] inlineData;

	public InlineContent(byte[] data) {
		inlineData = ArrayUtils.clone(data);
		length = inlineData.length;
	}

	/**
	 * Content that is represented by a byte sequence that can be infinitely
	 * repeated, and an offset and length parameter that define a segment from
	 * that sequence.
	 * 
	 * <p>
	 * E.g.
	 * 
	 * <pre>
	 * data       offset length     resulting content
	 * --------------------------------------------------------------------------
	 * Abcde      0      10         AbcdeAbcde (repeated)
	 *            0      2          Ab         (truncated)
	 *            0      8          AbcdeAbc   (truncated repetition)
	 *            3      10         deAbcdeAbc (truncated repetition with offset)
	 * </pre>
	 * 
	 */
	public InlineContent(byte[] data, int offset, int length) {
		this.length = length;
		offset %= data.length;
		if (offset == 0) {
			inlineData = Arrays.copyOf(data, Math.min(data.length, length));
			return;
		}
		byte[] shifted = new byte[data.length];
		System.arraycopy(data, offset, shifted, 0, data.length - offset);
		System.arraycopy(data, 0, shifted, offset + 1, offset);
		inlineData = shifted;
	}

	public static InlineContent deserialize(Map<String, Object> storageSchema) {
		MapUtils.supportedFields(storageSchema, "in", "length", "offset");
		byte[] data = (byte[]) storageSchema.get("in");
		if (data == null) {
			return new InlineContent(ArrayUtils.EMPTY_BYTE_ARRAY);
		}

		Long length = MapUtils.getLong(storageSchema, "length");
		Long offset = MapUtils.getLong(storageSchema, "offset");
		long l = length != null ? length : data.length;
		long o = offset != null ? offset : 0l;

		return new InlineContent(data, (int) o, (int) l);

	}

	public long getLength() {
		return length;
	}

	public InputStream getInputStream() {
		if (length <= inlineData.length)
			return new ByteArrayInputStream(inlineData, 0, (int) length);
		return new RepeatedInputStream();
	}

	public InputStream getInputStream(long offset, long length)
			throws IOException {
		if (offset < 0 && offset >= this.length)
			throw new IndexOutOfBoundsException("offset " + offset
					+ " is out of bounds");
		if (offset + length > this.length)
			throw new IndexOutOfBoundsException("length " + length
					+ " is out of bounds (offset: " + offset + ")");
		if (offset + length <= inlineData.length)
			return new ByteArrayInputStream(inlineData, (int) offset,
					(int) length);
		return new RepeatedInputStream((int) offset, length);
	}

	class RepeatedInputStream extends InputStream {

		private long remaining;

		private int pos = -1;

		RepeatedInputStream() {
			this.remaining = length;
		}

		RepeatedInputStream(int offset, long length) {
			pos = offset - 1;
			remaining = length;
		}

		@Override
		public int read() throws IOException {
			if (remaining > 0) {
				remaining--;
				pos++;
				if (pos >= inlineData.length)
					pos = 0;

				return inlineData[pos];
			}
			return -1;
		}

	}

	public Map<String, Object> serialize() {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("in", inlineData.clone());
		if (length != inlineData.length)
			result.put("length", length);
		return result;
	}

	public boolean contentEquals(ContentPointer otherContent) {
		if (otherContent instanceof InlineContent) {
			InlineContent ic = (InlineContent) otherContent;
			return length == ic.length
					&& Arrays.equals(inlineData, ic.inlineData);
		}
		if (otherContent == null || otherContent.getLength() != length)
			return false;
		return otherContent.contentEquals(this);
	}

}
