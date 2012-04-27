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

public class OffsetAndLengthTest extends TestCase {

	public void testContentRepetition() throws IOException {

		Content doubled = new OffsetAndLength(new InlineContent("abcde"
				.getBytes()), 0, 10);

		assertEquals("abcdeabcde", IOUtils.toString(doubled.getInputStream()));
		assertEquals("cdeabc", IOUtils.toString(doubled.getInputStream(2, 6)));
		assertEquals(10l, doubled.getLength());
	}

	public void testContentRepetitionWithOffset() throws IOException {

		Content doubled = new OffsetAndLength(new InlineContent("abcde"
				.getBytes()), 3, 10);

		assertEquals("deabcdeabc", IOUtils.toString(doubled.getInputStream()));
		assertEquals("eabcde", IOUtils.toString(doubled.getInputStream(1, 6)));
		assertEquals(10l, doubled.getLength());
	}
}
