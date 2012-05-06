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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

public final class ContentSHA implements ContentPointer {

	private final byte[] sha;

	private final Long length;

	private ContentSHA(byte[] sha, long length) {
		this.sha = sha;
		this.length = length;
	}

	public byte[] getSHA() {
		return sha.clone();
	}

	/**
	 * @return the hex-encoded SHA
	 */
	public String getDigest() {
		return Hex.encodeHexString(sha);
	}

	public long getLength() {
		return length;
	}

	public static ContentSHA forDigestAndLength(byte[] sha, long length) {
		return new ContentSHA(sha.clone(), length);
	}

	public static ContentSHA calculate(byte[] data) {
		return new ContentSHA(DigestUtils.sha(data), data.length);
	}

	public static ContentSHA calculate(byte[] data, int offset, int length) {
		try {
			return new ContentSHA(DigestUtils.sha(new ByteArrayInputStream(
					data, offset, length)), length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, Object> serialize() {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("sha", getSHA());
		if (length != null)
			result.put("length", length);
		return result;
	}

	public boolean contentEquals(ContentPointer otherContent) {
		if (otherContent == null || otherContent.getLength() != length)
			return false;
		if (otherContent instanceof ContentSHA)
			return Arrays.equals(sha, ((ContentSHA) otherContent).sha);
		if (otherContent instanceof StoredContent) {
			StoredContent s = (StoredContent) otherContent;
			return s.getOffset() == 0 && Arrays.equals(s.getBaseSHA(), sha);
		}
		if (otherContent instanceof InlineContent)
			try {
				return Arrays.equals(sha, DigestUtils
						.sha(((InlineContent) otherContent).getInputStream()));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		return false;
	}
}
