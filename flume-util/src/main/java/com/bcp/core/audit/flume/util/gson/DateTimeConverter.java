package com.bcp.core.audit.flume.util.gson;

import java.lang.reflect.Type;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public final class DateTimeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
	
	private static final String ISO_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern(ISO_PATTERN);

	@Override
	public JsonElement serialize(DateTime src, Type srcType, JsonSerializationContext context) {
		return new JsonPrimitive(src.toString(FORMATTER));
	}

	@Override
	public DateTime deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
		try {
			return new DateTime(json.getAsString());
		} catch (IllegalArgumentException e) {
			// May be it came in formatted as a java.util.Date, so try that
			Date date = context.deserialize(json, Date.class);
			return new DateTime(date);
		}
	}
}
