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
package v7db.files.mongodb;

import java.io.IOException;

import jmockmongo.MockMongoTestCaseSupport;

import org.bson.BasicBSONObject;

public class V7FileTest extends MockMongoTestCaseSupport {

	private V7GridFS gridFS;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		// the root folder
		prepareMockData("test.v7files.files",
				new BasicBSONObject("_id", "root"));
		gridFS = new V7GridFS(getMongo().getDB("test"));
	}

	public void testRename() throws IOException {
		gridFS.addFile("abc".getBytes(), "root", "test.dat", "text/plain");
		V7File file = gridFS.getFile("root", "test.dat");
		file.rename("newName.dat");
		assertEquals("newName.dat", file.getName());
		assertEquals("root", file.getParent().getId());
		V7File check = gridFS.getFile("root", "newName.dat");
		assertEquals(file.getId(), check.getId());
		assertNull(gridFS.getFile("root", "test.dat"));
	}

	public void testConflict() throws IOException {
		gridFS.addFile("abc".getBytes(), "root", "test.dat", "text/plain");
		V7File file1 = gridFS.getFile("root", "test.dat");
		V7File file2 = gridFS.getFile("root", "test.dat");
		file1.rename("newName.dat");
		try {
			file2.rename("too-late");
			fail();
		} catch (IOException e) {
			assertTrue(e.getMessage().contains("UpdateConflict"));
		}
		assertNull(gridFS.getFile("root", "test.dat"));
		assertNull(gridFS.getFile("root", "too-late"));
	}

}
