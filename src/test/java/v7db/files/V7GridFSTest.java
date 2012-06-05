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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.io.IOUtils;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import v7db.files.mongodb.V7File;
import v7db.files.mongodb.V7GridFS;

public class V7GridFSTest extends MockMongoTestCaseSupport {

	private V7GridFS gridFS;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		// the root folder
		prepareMockData("test.v7files.files",
				new BasicBSONObject("_id", "root"));
		gridFS = new V7GridFS(getMongo().getDB("test"));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	private void testSimpleScenario(byte[] fileData) throws IOException {

		// insert

		gridFS.addFile(fileData, "root", "test.dat", "text/plain");
		V7File file = gridFS.getFile("root", "test.dat");
		assertEquals("test.dat", file.getName());
		assertEquals("text/plain", file.getContentType());
		assertEquals(Arrays.toString(fileData), Arrays.toString(IOUtils
				.toByteArray(file.getInputStream())));
		assertEquals(fileData.length, file.getLength().intValue());
		assertEquals(1, file.getVersion());
		assertEquals("root", file.getParentId());

		V7File root = file.getParent();
		assertEquals("root", root.getId());
		List<V7File> files = root.getChildren();
		assertEquals(1, files.size());

		// update
		// TODO: jmockmongo needs findAndModify first

		// file.setContent("updated contents".getBytes(), "text/plain");
		//
		// file = gridFS.getFile("root", "test.dat");
		// assertEquals("test.dat", file.getName());
		// assertEquals("text/plain", file.getContentType());
		// assertEquals("updated contents", IOUtils
		// .toString(file.getInputStream()));

		// delete
		file.delete();
		assertNull(gridFS.getFile("root", "test.dat"));

	}

	public void testSimpleScenario_ShortFile() throws IOException {
		testSimpleScenario("a short file".getBytes());
	}

	public void testSimpleScenario_LongFile() throws IOException {
		byte[] bytes = new byte[1000000];
		new Random(12345).nextBytes(bytes);
		testSimpleScenario(bytes);
	}

	public void testFolder() throws IOException {
		ObjectId folderId = gridFS.addFolder("root", "foolder");
		V7File folder = gridFS.getFile("root", "foolder");
		assertEquals("foolder", folder.getName());
		assertEquals(folderId, folder.getId());
		assertEquals(false, folder.hasContent());

		V7File file = folder.createChild("a file in there".getBytes(),
				"file.txt", "text/plain");
		assertEquals(folder.getId(), file.getParentId());
		V7File checkFile = gridFS.getFile("root", "foolder", "file.txt");
		assertEquals(file.getId(), checkFile.getId());
		assertEquals(folder.getId(), checkFile.getParentId());

	}
}
