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

import java.util.Map;

/**
 * A ContentPointer describes how a piece of content is stored, either (for very
 * small data) inlined into the pointer, or inside of the ContentStorage,
 * referenced by a SHA-1 digest (or the content itself or another piece of
 * content that contains it).
 * <p>
 * ContentPointers are how you interact with the ContentStorage system: When you
 * store some data, you are given a ContentPointer. Hold on to that
 * ContentPointer (can be serialized as a BSON document) to later retrieve your
 * data.
 * 
 * @see {@link InlineContent}, {@link StoredContent}
 */

public interface ContentPointer {

	/**
	 * @return a Map that can be put into a BSON document
	 */
	Map<String, Object> serialize();

	/**
	 * 
	 * @return true, if the content SHA-1 equals, but false even if that is
	 *         unknown
	 */
	public boolean contentEquals(ContentPointer otherContent);

	public long getLength();

}
