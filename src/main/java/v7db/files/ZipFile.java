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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import net.lingala.zip4j.core.HeaderReader;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.LocalFileHeader;
import net.lingala.zip4j.model.ZipModel;
import net.lingala.zip4j.unzip.UnzipEngine;

/**
 * Utility to store and index a ZipFile.
 * <p>
 * In addition to just storing the ZipFile as a whole, references to all the
 * contents of all its files are also added to the database (so that if one of
 * those is uploaded later separately, it will not take up extra storage).
 * <p>
 * Cannot be used for files protected by password or otherwise encrypted.
 * 
 * 
 */

public class ZipFile {

	public static final String CONTENT_TYPE = "application/zip";

	public static Object addZipFile(V7GridFS fs, File zipFile,
			Object parentFileId, String filename) throws IOException {
		// make sure we have the contents first
		byte[] sha = fs.insertContents(zipFile, filename, null, CONTENT_TYPE);

		try {
			// open up the zip file
			HeaderReader r = new HeaderReader(
					new RandomAccessFile(zipFile, "r"));
			ZipModel model = r.readAllHeaders();
			model.setZipFile(filename);

			// index all component files
			List<?> fhs = model.getCentralDirectory().getFileHeaders();
			for (Object _fh : fhs) {
				FileHeader fh = (FileHeader) _fh;
				UnzipEngine en = new UnzipEngine(model, fh);
				// this will read the local file header
				en.getInputStream();
				LocalFileHeader lh = en.getLocalFileHeader();

				Concatenation.storeConcatenation(fs, lh.getFileName(), null,
						null, Concatenation.zipEntryInGridFSContents(sha,
								(int) fh.getOffsetLocalHeader(), (int) (lh
										.getOffsetStartOfData() - fh
										.getOffsetLocalHeader()), (int) lh
										.getCompressedSize()));

			}
		} catch (ZipException e) {
			throw new IOException("failed to index zip file " + zipFile, e);
		}
		// the finally add the zip file to the file system tree
		return fs.addFile(zipFile, parentFileId, filename, CONTENT_TYPE);
	}

}
