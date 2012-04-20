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

/**
 * A content storage system that also keeps track of references for the content,
 * i.e. who is using the content. This is necessary to identify and delete "old"
 * content that falls out of use.
 * 
 */

public interface ReferenceTracking {

	/**
	 * Call this method when storing a ContentPointer.
	 * <p>
	 * This will create a reference to these contents to prevent them from being
	 * garbage collected.
	 * <p>
	 * Call the method again when updating the owner document (with new
	 * contents).
	 * <p>
	 * References will not be deleted until {@link #purge(Object)} is called for
	 * the owner document.
	 */
	void updateReferences(Object ownerId, ContentPointer... contents)
			throws IOException;

	/**
	 * Call this method after deleting the owner document.
	 * <p>
	 * This will delete all references associated with it.
	 * <p>
	 * This cannot be undone.
	 */
	void purge(Object ownerId) throws IOException;

}
