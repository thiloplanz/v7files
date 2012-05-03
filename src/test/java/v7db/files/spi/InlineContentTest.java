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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

public class InlineContentTest extends TestCase {

	public void testInlineContent() throws IOException {
		assertInlineContent("abcd", new InlineContent("abcd".getBytes()));
	}

	public void testEmpty() throws IOException {
		assertInlineContent("", new InlineContent("".getBytes()));
	}

	public void testContentRepetition() throws IOException {
		Content doubled = new InlineContent("abcde".getBytes(), 0, 10);
		assertInlineContent("abcdeabcde", doubled);
	}

	public void testContentRepetitionWithOffset() throws IOException {
		Content doubled = new InlineContent("abcde".getBytes(), 2, 10);
		assertInlineContent("cdeabcdeab", doubled);
	}

	public void testContentEquals() {
		ContentPointer abc = new InlineContent("abc".getBytes());

		assertTrue(abc.contentEquals(abc));
		assertTrue(abc.contentEquals(new InlineContent("abc".getBytes())));
		ContentSHA sha = ContentSHA.calculate("abc".getBytes());
		assertTrue(abc.contentEquals(sha));
		assertTrue(abc.contentEquals(new StoredContent(sha.getSHA(), abc
				.getLength())));

		assertFalse(abc.contentEquals(null));
		assertFalse(abc.contentEquals(new InlineContent("abcd".getBytes())));
		assertFalse(abc.contentEquals(ContentSHA.calculate("xyz".getBytes())));
	}

	private void assertInlineContent(String content, Content inlineContent)
			throws IOException {
		assertNotNull(inlineContent);
		assertEquals(content, IOUtils.toString(inlineContent.getInputStream()));
		assertEquals(content.length(), inlineContent.getLength());
		if (content.length() > 2)
			assertEquals(content.substring(1, content.length() - 1), IOUtils
					.toString(inlineContent.getInputStream(1,
							content.length() - 2)));
	}

}
