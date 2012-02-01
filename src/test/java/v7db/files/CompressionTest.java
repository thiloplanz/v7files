/**
 * Copyright (c) 2011-2012, Thilo Planz. All rights reserved.
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

package v7db.files;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.DataFormatException;

import junit.framework.TestCase;

public class CompressionTest extends TestCase {

	public void testDeflateBytes() throws DataFormatException {
		byte[] data = { 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3 };
		{
			byte[] out = new byte[1000];
			int len = Compression.deflate(data, 0, data.length, out);
			byte[] inflated = new byte[data.length];
			Compression.inflate(out, 0, len, inflated);
			assertEquals(Arrays.toString(data), Arrays.toString(inflated));
			assertEquals(7, len);
		}

		{
			byte[] tooSmall = new byte[5];
			int len = Compression.deflate(data, 0, data.length, tooSmall);
			assertEquals(0, len);
		}
	}

	public void testGZipBytes() throws DataFormatException, IOException {
		byte[] data = { 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3,
				1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3 };
		{
			byte[] out = Compression.gzip(data, 0, data.length);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Compression.gunzip(new ByteArrayInputStream(out), baos);

			assertEquals(Arrays.toString(data), Arrays.toString(baos
					.toByteArray()));
			assertEquals(25, out.length);
		}

		{

			assertNull("do not gzip data if that makes it larger", Compression
					.gzip(data, 0, 5));
		}
	}

}
