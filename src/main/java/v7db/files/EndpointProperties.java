/**
 * Copyright (c) 2011, Thilo Planz. All rights reserved.
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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Enumeration;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

class EndpointProperties extends Properties {

	private static final long serialVersionUID = 1L;

	private final String endpoint;

	EndpointProperties(String endpoint, Properties props) {
		super(props);
		this.endpoint = endpoint;
	}

	@Override
	public String getProperty(String key) {
		String p = super.getProperty(endpoint + "." + key);
		if (p == null)
			return super.getProperty(key);
		return p;
	}

	@Override
	public void list(PrintStream out) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void list(PrintWriter out) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void load(InputStream inStream) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void load(Reader reader) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void loadFromXML(InputStream in) throws IOException,
			InvalidPropertiesFormatException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Enumeration<?> propertyNames() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void save(OutputStream out, String comments) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized Object setProperty(String key, String value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void store(OutputStream out, String comments) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void store(Writer writer, String comments) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void storeToXML(OutputStream os, String comment,
			String encoding) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void storeToXML(OutputStream os, String comment)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> stringPropertyNames() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized Object clone() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized boolean contains(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized Enumeration<Object> elements() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<Object, Object>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized Object put(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized Object remove(Object key) {
		throw new UnsupportedOperationException();
	}

}
