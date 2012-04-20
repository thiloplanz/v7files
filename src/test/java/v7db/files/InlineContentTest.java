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

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

import v7db.files.spi.InlineContent;

public class InlineContentTest extends TestCase {

	private void assertInlineContent(String content, InlineContent inlineContent)
			throws IOException {
		assertNotNull(inlineContent);
		assertEquals(content, IOUtils.toString(inlineContent.getInputStream()));
		assertEquals(content.length(), inlineContent.getLength());

	}

	public void testInlineContent() throws IOException {
		assertInlineContent("abcd", new InlineContent("abcd".getBytes()));

	}

	public void testRepeatedInlineContent() throws IOException {
		assertInlineContent("abcdabcdab", new InlineContent("abcd".getBytes(),
				0, 10));

	}

}
