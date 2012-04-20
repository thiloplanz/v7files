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

package v7db.files;

import static org.apache.commons.codec.binary.Hex.decodeHex;

import java.io.IOException;

import jmockmongo.MockMongoTestCaseSupport;
import net.lingala.zip4j.exception.ZipException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.digest.DigestUtils;

import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.ContentPointer;
import v7db.files.spi.ContentStorage;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class ZipFileTest extends MockMongoTestCaseSupport {

	public void testPullOutFileFromZip() throws MongoException, IOException,
			ZipException, DecoderException {

		ContentStorage storage = new MongoContentStorage(new Mongo()
				.getDB("test"));

		ContentPointer zip = storage.storeContent(getClass()
				.getResourceAsStream("mongodb.epub"));

		ContentPointer png = ZipFile.extractFile(storage, zip,
				"images/img0.png");

		assertEquals("fc012bb0439382f709d3caebab958ff592811d17", DigestUtils
				.shaHex(storage.getContent(png).getInputStream()));

	}

	public void testIndexZipFile() throws MongoException, IOException,
			ZipException, DecoderException {

		ContentStorage storage = new MongoContentStorage(new Mongo()
				.getDB("test"));

		ContentPointer zip = storage.storeContent(getClass()
				.getResourceAsStream("mongodb.epub"));

		ZipFile.index(storage, zip);

		assertEquals("fc012bb0439382f709d3caebab958ff592811d17", DigestUtils
				.shaHex(storage.getContent(
						decodeHex("fc012bb0439382f709d3caebab958ff592811d17"
								.toCharArray())).getInputStream()));

	}
}
