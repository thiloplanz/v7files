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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import jmockmongo.MockMongoTestCaseSupport;
import v7db.files.mongodb.MongoContentStorage;
import v7db.files.spi.ContentSHA;

public class CatCommandTest extends MockMongoTestCaseSupport {

	private final ByteArrayOutputStream out = new ByteArrayOutputStream(10000);

	private PrintStream realOut;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		UnitTestSupport.initConfiguration(UnitTestSupport
				.getDefaultProperties());
		realOut = System.out;
		System.setOut(new PrintStream(out));
	}

	public void testSha() throws IOException {
		MongoContentStorage storage = new MongoContentStorage(getMongo().getDB(
				"test"));
		ContentSHA sha = storage.storeContent(new ByteArrayInputStream("test"
				.getBytes()));
		CatCommand.main(new String[] { "cat", "-sha", sha.getDigest() });
		System.out.flush();
		assertEquals("test", out.toString());
	}

	@Override
	protected void tearDown() throws Exception {
		System.setOut(realOut);
		super.tearDown();
	}

}
