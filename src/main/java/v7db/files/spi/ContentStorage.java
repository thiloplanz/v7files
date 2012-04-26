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
import java.io.InputStream;
import java.util.Map;

/**
 * The interface to store contents.
 * 
 * <p>
 * Implementations need to provide the following features:
 * <ul>
 * <li>Storing content given as an InputStream, returning the SHA digest to
 * later retrieve it.
 * <li>Retrieving stored content by its SHA digest.
 * <li>Retrieving means "producing an InputStream to read the contents" and
 * "providing information about content length".
 * <li>Retrieving parts of contents as referenced by "content pointer"
 * documents.
 * </ul>
 * 
 * 
 * @see https://github.com/thiloplanz/v7files/wiki/StorageFormat
 * 
 */

public interface ContentStorage {

	/**
	 * will close the InputStream before returning
	 */
	ContentSHA storeContent(InputStream data) throws IOException;

	/**
	 * store "out-of-band" content.
	 * 
	 * @throws UnsupportedOperationException
	 *             if the specified scheme is unknown
	 */

	ContentPointer storeContent(Map<String, Object> storageScheme)
			throws IOException;

	/**
	 * @return null, if no such content was stored
	 */
	Content getContent(byte[] sha) throws IOException;

	/**
	 * @return null, if no such content was stored (or the pointer is null)
	 */
	Content getContent(ContentPointer pointer) throws IOException;

	/**
	 * 
	 * @throws UnsupportedOperationException
	 *             if the specified scheme is unknown
	 */
	Content getContent(Map<String, Object> storageScheme) throws IOException;

}
