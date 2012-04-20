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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.lingala.zip4j.core.HeaderReader;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.LocalFileHeader;
import net.lingala.zip4j.model.ZipModel;
import net.lingala.zip4j.unzip.UnzipEngine;

import org.apache.commons.io.IOUtils;

import v7db.files.spi.Content;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentStorage;
import v7db.files.spi.InlineContent;
import v7db.files.spi.StorageScheme;

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

	public static final class ContentFromZipFile implements StorageScheme {

		public Content getContent(ContentStorage storage,
				Map<String, Object> data) throws IOException {
			byte[] base = MapUtils.getRequiredBytes(data, "base.sha");
			long offset = MapUtils.getRequiredLong(data, "off");
			long length = MapUtils.getRequiredLong(data, "end") - offset;
			byte[] unzipped = IOUtils.toByteArray(Compression.unzip(storage
					.getContent(base).getInputStream(offset, length)));
			return new InlineContent(unzipped);
		}

		public String getId() {
			return "zip";
		}

	}

	/**
	 * index all individual files found in a zip archive already in storage
	 * 
	 * @throws IOException
	 */
	public static final void index(ContentStorage storage,
			ContentPointer zipFile) throws IOException {
		Content zip = storage.getContent(zipFile);
		if (zip == null)
			throw new IllegalArgumentException("invalid ContentPointer "
					+ zipFile);

		File tmp = File.createTempFile("v7files_zipfile_extractfile_", ".zip");
		try {
			OutputStream f = new FileOutputStream(tmp);
			IOUtils.copy(zip.getInputStream(), f);
			f.close();

			// open up the zip file
			HeaderReader r = new HeaderReader(new RandomAccessFile(tmp, "r"));
			ZipModel model = r.readAllHeaders();
			model.setZipFile(tmp.getAbsolutePath());
			Map<String, Object> map = zipFile.serialize();
			List<?> fhs = model.getCentralDirectory().getFileHeaders();
			for (Object _fh : fhs) {
				FileHeader fh = (FileHeader) _fh;
				UnzipEngine en = new UnzipEngine(model, fh);
				// this will read the local file header
				en.getInputStream();
				LocalFileHeader lh = en.getLocalFileHeader();

				store(storage, map, fh, lh);

			}

		} catch (ZipException e) {
			throw new IllegalArgumentException(
					"ContentPointer does not refer to a zip file: " + zipFile,
					e);
		} finally {
			tmp.delete();
		}

	}

	/**
	 * find the data indicated by the ContentPointer, treats it as a zip
	 * archive, extracts the named file inside the archive, stores a reference
	 * to it in the ContentStorage and returns a ContentPointer to it.
	 * 
	 * @throws FileNotFoundException
	 *             if the archive exists, but does not contain the named file
	 * @throws IllegalArgumentException
	 *             if the ContentPointer does not refer to a zip archive
	 * 
	 */
	public static final ContentPointer extractFile(ContentStorage storage,
			ContentPointer zipFile, String fileName) throws IOException {
		Content zip = storage.getContent(zipFile);
		if (zip == null)
			throw new IllegalArgumentException("invalid ContentPointer "
					+ zipFile);

		File tmp = File.createTempFile("v7files_zipfile_extractfile_", ".zip");
		try {
			OutputStream f = new FileOutputStream(tmp);
			IOUtils.copy(zip.getInputStream(), f);
			f.close();

			// open up the zip file
			HeaderReader r = new HeaderReader(new RandomAccessFile(tmp, "r"));
			ZipModel model = r.readAllHeaders();
			model.setZipFile(tmp.getAbsolutePath());

			List<?> fhs = model.getCentralDirectory().getFileHeaders();
			for (Object _fh : fhs) {
				FileHeader fh = (FileHeader) _fh;
				if (fileName.equals(fh.getFileName())) {
					UnzipEngine en = new UnzipEngine(model, fh);
					// this will read the local file header
					en.getInputStream();
					LocalFileHeader lh = en.getLocalFileHeader();
					return store(storage, zipFile.serialize(), fh, lh);
				}

			}

		} catch (ZipException e) {
			throw new IllegalArgumentException(
					"ContentPointer does not refer to a zip file: " + zipFile,
					e);
		} finally {
			tmp.delete();
		}
		throw new FileNotFoundException("ContentPointer does not contain "
				+ fileName + ": " + zipFile);
	}

	private static ContentPointer store(ContentStorage storage,
			Map<String, Object> zipFile, FileHeader fh, LocalFileHeader lh)
			throws IOException {
		Map<String, Object> cat = new HashMap<String, Object>();
		cat.put("store", "zip");
		cat.put("base", zipFile);
		cat.put("off", fh.getOffsetLocalHeader());
		cat.put("length", fh.getUncompressedSize());
		cat.put("end", lh.getOffsetStartOfData() + lh.getCompressedSize());
		return storage.storeContent(cat);
	}

}
