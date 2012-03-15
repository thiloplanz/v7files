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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Compression {

	// minimum overhead: 10 byte header, 8 byte trailer
	static final int GZIP_STORAGE_OVERHEAD = 18;

	static Logger log = LoggerFactory.getLogger(Compression.class);

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
	 * @return null, if the "gzipped" data is larger than the input (or there
	 *         has been an exception)
	 */
	static byte[] gzip(byte[] data, int off, int len) {

		if (len < GZIP_STORAGE_OVERHEAD)
			return null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
			GZIPOutputStream gz = new GZIPOutputStream(baos);
			gz.write(data, off, len);
			gz.close();
			if (baos.size() >= len)
				return null;
			return baos.toByteArray();
		} catch (Exception e) {
			log.error("failed to gzip byte array", e);
			return null;
		}
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

	static void gunzip(InputStream in, OutputStream out) throws IOException {
		IOUtils.copy(new GZIPInputStream(in), out);
	}

	/**
	 * input must be in zip format, i.e. first a local header, and then the data
	 * segment.
	 * 
	 */
	static InputStream unzip(InputStream in) throws IOException {
		ZipInputStream zip = new ZipInputStream(in);
		zip.getNextEntry();
		return zip;
	}

	/**
	 * @return the compressed file (delete after use), or null, if the
	 *         "compressed" data is bigger than the input (or if there was an
	 *         exception)
	 */

	public static File gzip(File data) {
		File file = null;
		try {
			file = File.createTempFile(data.getName(), ".gz");
			OutputStream out = new GZIPOutputStream(new FileOutputStream(file));
			long size = FileUtils.copyFile(data, out);
			out.close();
			long cSize = file.length();
			if (cSize < size) {
				return file;
			}
			file.delete();
			return null;
		} catch (Exception e) {
			log.error("failed to gzip file " + data, e);
			if (file != null)
				file.delete();
			return null;
		}
	}

}
