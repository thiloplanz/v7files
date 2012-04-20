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

public final class LazyLoadedContent implements Content {

	private final ContentStorage storage;

	private final StoredContent contentPointer;

	public LazyLoadedContent(ContentStorage storage,
			StoredContent contentPointer) {
		this.storage = storage;
		this.contentPointer = contentPointer;
	}

	public InputStream getInputStream() throws IOException {
		return storage.getContent(contentPointer).getInputStream();
	}

	public InputStream getInputStream(long offset, long length)
			throws IOException {
		return storage.getContent(contentPointer)
				.getInputStream(offset, length);
	}

	public long getLength() {
		return contentPointer.getLength();
	}

}
