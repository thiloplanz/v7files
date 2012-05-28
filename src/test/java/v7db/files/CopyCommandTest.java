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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;

import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.ContentSHA;

public class CopyCommandTest extends MockMongoTestCaseSupport {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		UnitTestSupport.initConfiguration(UnitTestSupport
				.getDefaultProperties());

	}

	public void testSHA() throws IOException {
		MongoContentStorage storage = new MongoContentStorage(getMongo().getDB(
				"test"));
		ContentSHA sha = storage.storeContent(new ByteArrayInputStream("test"
				.getBytes()));

		CopyCommand.main(new String[] { "copy", "-sha", sha.getDigest(), "x",
				"copy.txt" });

		V7GridFS fs = new V7GridFS(getMongo().getDB("test"));
		V7File file = fs.getFile("x", "copy.txt");
		assertEquals("test", IOUtils.toString(file.getInputStream()));

	}

	public void testCopyFile() throws IOException {
		V7GridFS fs = new V7GridFS(getMongo().getDB("test"));
		ObjectId dir = fs.addFolder("test", "folder");
		fs.addFile("test".getBytes(), dir, "test.txt", "text/plain");

		CopyCommand.main(new String[] { "copy", "test", "folder/test.txt", "x",
				"copy.txt" });

		V7File file = fs.getFile("x", "copy.txt");
		assertEquals("test", IOUtils.toString(file.getInputStream()));
		assertEquals("text/plain", file.getContentType());
	}

	public void testCopyDirectory() throws IOException {
		V7GridFS fs = new V7GridFS(getMongo().getDB("test"));
		ObjectId dir = fs.addFolder("test", "folder");
		fs.addFile("test".getBytes(), dir, "test.txt", "text/plain");

		CopyCommand
				.main(new String[] { "copy", "test", "folder", "x", "copy" });

		V7File file = fs.getFile("x", "copy", "test.txt");
		assertEquals("test", IOUtils.toString(file.getInputStream()));
		assertEquals("text/plain", file.getContentType());
	}

}
