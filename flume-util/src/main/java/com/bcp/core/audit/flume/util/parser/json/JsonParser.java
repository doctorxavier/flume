package com.bcp.core.audit.flume.util.parser.json;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.bcp.core.audit.flume.util.gson.DateTimeConverter;
import com.bcp.core.audit.flume.util.gson.LocalDateConverter;
import com.bcp.core.audit.flume.util.parser.Parser;
import com.bcp.core.audit.flume.util.parser.exception.ParserException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonParser<E> implements Parser<E> {
	
	private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
			.registerTypeAdapter(DateTime.class, new DateTimeConverter())
			.registerTypeAdapter(LocalDate.class, new LocalDateConverter()).create();
	
	private Class<E> clazz;
	
	private JsonParser() {
		
	}
	
	public JsonParser(Class<E> clazz) {
		this();
		this.clazz = clazz;
	}
	
	public JsonParser(Class<E> clazz, boolean prettyPrinting) {
		this(clazz);
		if (prettyPrinting) {
			buildParser();
		}
	}
	
	private static Gson buildParser() {
		return gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
		.registerTypeAdapter(DateTime.class, new DateTimeConverter())
		.registerTypeAdapter(LocalDate.class, new LocalDateConverter()).setPrettyPrinting().create();
	}
	
	@Override
	public E unmarshall(String inPath) throws ParserException {
		E response = null;
		InputStream in = null;
		try {
			in = Thread.currentThread().getContextClassLoader().getResourceAsStream(inPath);
			response = this.unmarshall(in);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					throw new ParserException(e);
				}
			}
		}
		return response;
	}

	@Override
	public E unmarshall(InputStream in) throws ParserException {
		E response = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
			response = gson.fromJson(br, this.clazz);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					throw new ParserException(e);
				}
			}
		}
		return response;
	}
	
	public E unmarshallFromString(String marshall) {
		return gson.fromJson(marshall, this.clazz);
	}

	@Override
	public void marshall(E marshall, OutputStream out) throws ParserException {
		String json = gson.toJson(marshall);
		try {
			out.write(json.getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			throw new ParserException(e);
		}
	}

	@Override
	public String toString(E marshall) throws ParserException {
		return gson.toJson(marshall);
	}

	@Override
	public String toString(E marshall, int indent) throws ParserException {
		return this.toString(marshall);
	}

}
