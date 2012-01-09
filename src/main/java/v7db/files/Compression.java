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

import static java.util.zip.Deflater.BEST_COMPRESSION;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

import org.apache.commons.io.FileUtils;

class Compression {

	/**
	 * @return 0, if the "deflated" data fills the whole output array
	 */
	static int deflate(byte[] data, int off, int len, byte[] out) {
		Deflater deflater = new Deflater(BEST_COMPRESSION, true);
		deflater.setInput(data, off, len);
		deflater.finish();
		int size = deflater.deflate(out);
		if (size == 0 || size == out.length)
			return 0;
		return size;
	}

	/**
	 * assumes that the result buffer has exactly the needed size
	 * 
	 * @throws DataFormatException
	 */
	static void inflate(byte[] data, int off, int len, byte[] out)
			throws DataFormatException {
		Inflater inflater = new Inflater(true);
		inflater.setInput(data, off, len);
		int size = inflater.inflate(out);
		if (size != out.length)
			throw new DataFormatException("unexpected size of deflated data: "
					+ size + " instead of " + out.length);
	}

	/**
	 * @return null, if the "deflated" data is bigger than the input, or the
	 *         compressed file (delete after use)
	 * @throws IOException
	 */

	static File deflate(File data) throws IOException {
		File file = File.createTempFile(data.getName(), ".z");
		OutputStream out = new DeflaterOutputStream(new FileOutputStream(file),
				new Deflater(BEST_COMPRESSION, true));
		long size = FileUtils.copyFile(data, out);
		out.close();
		long cSize = file.length();
		if (cSize < size) {
			return file;
		}
		file.delete();
		return null;
	}

}
