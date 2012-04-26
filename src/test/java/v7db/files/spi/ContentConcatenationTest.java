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
package v7db.files.spi;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import jmockmongo.MockMongoTestCaseSupport;

import org.apache.commons.io.IOUtils;
import org.bson.BasicBSONObject;

import v7db.files.mongodb.MongoContentStorage;

public class ContentConcatenationTest extends MockMongoTestCaseSupport {

	public void testContentRepetition() throws IOException {

		ContentStorage storage = new MongoContentStorage(getMongo().getDB(
				"test"));
		{
			Content doubled = new ContentConcatenation().getContent(storage,
					new BasicBSONObject("store", "cat").append("base",
							new InlineContent("abcde".getBytes(), 0, 10)
									.serialize()));

			assertEquals("abcdeabcde", IOUtils.toString(doubled
					.getInputStream()));
			assertEquals(10l, doubled.getLength());
		}
		{
			ContentSHA one = storage.storeContent(new ByteArrayInputStream(
					"abcde".getBytes()));

			Content doubled = new ContentConcatenation().getContent(storage,
					new BasicBSONObject("store", "cat").append("base",
							new BasicBSONObject("store", "cat").append("base",
									new StoredContent(one.getSHA(), 10)
											.serialize())));

			assertEquals("abcdeabcde", IOUtils.toString(doubled
					.getInputStream()));
			assertEquals(10l, doubled.getLength());
		}
		{
			ContentSHA one = storage.storeContent(new ByteArrayInputStream(
					"abcde".getBytes()));

			Content doubled = new ContentConcatenation().getContent(storage,
					new BasicBSONObject("store", "cat").append("base",
							new BasicBSONObject("store", "cat").append("base",
									new Object[] {
											"xxxx".getBytes(),
											new StoredContent(one.getSHA(), 10)
													.serialize() })));

			assertEquals("xxxxabcdeabcde", IOUtils.toString(doubled
					.getInputStream()));
			assertEquals(14l, doubled.getLength());
		}

	}
}
