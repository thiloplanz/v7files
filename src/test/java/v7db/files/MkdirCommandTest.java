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

import jmockmongo.MockMongoTestCaseSupport;

public class MkdirCommandTest extends MockMongoTestCaseSupport {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		UnitTestSupport.initConfiguration(UnitTestSupport
				.getDefaultProperties());

	}

	public void testSimple() throws IOException {
		V7GridFS fs = new V7GridFS(getMongo().getDB("test"));
		MkdirCommand.main(new String[] { "mkdir", "x", "123" });
		{
			V7File check = fs.getFile("x", "123");
			assertFalse(check.hasContent());
			assertEquals("[]", check.getChildren().toString());
		}
		MkdirCommand.main(new String[] { "mkdir", "x", "123/456" });
		{
			V7File check = fs.getFile("x", "123", "456");
			assertFalse(check.hasContent());
			assertEquals("[]", check.getChildren().toString());
		}
	}

	public void testExisting() throws IOException {
		V7GridFS fs = new V7GridFS(getMongo().getDB("test"));
		fs.addFolder("x", "123");
		try {
			MkdirCommand.main(new String[] { "mkdir", "x", "123" });
			fail("should have croaked on existing directory");
		} catch (IOException e) {

		}
	}
}
