package com.bcp.core.audit.flume.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;

public final class PojoUtils {
	
	// CHECKSTYLE:OFF
    private static int RANDOM_LENGTH = 10;
    // CHECKSTYLE:ON

    private PojoUtils() {
    	
    }
    
	public static void initializePojo(final Object object) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		for (Method method : object.getClass().getMethods()) {
			if (Modifier.isPublic(method.getModifiers())) {
				if ("void".equals(method.getReturnType().getName()) && "set".equals(method.getName().substring(0, 3)) && method.getParameters().length == 1) {
					
					final String name = method.getName().substring(3).toLowerCase(Locale.ENGLISH);
					
					//CHECKSTYLE:OFF
					final Class<?> parameterType = method.getParameterTypes()[0];
					//CHECSTYLE:ON
					
					if (parameterType.isAssignableFrom(Boolean.class) || "bool".equals(parameterType.getName())) {
						method.invoke(object, RandomUtils.nextBoolean());
					}
					if (parameterType.isAssignableFrom(Integer.class) || "int".equals(parameterType.getName())) {
						method.invoke(object, RandomUtils.nextInt());
					}
					else if (parameterType.isAssignableFrom(Long.class) || "long".equals(parameterType.getName())) {
						method.invoke(object, RandomUtils.nextLong());
					}
					else if (parameterType.isAssignableFrom(Float.class) || "float".equals(parameterType.getName())) {
						method.invoke(object, RandomUtils.nextFloat());
					}
					else if (parameterType.isAssignableFrom(Double.class) || "double".equals(parameterType.getName())) {
						method.invoke(object, RandomUtils.nextDouble());
					}
					else if (parameterType.isAssignableFrom(Date.class)) {
						method.invoke(object, new Date());
					}
					else if (parameterType.isAssignableFrom(DateTime.class)) {
						method.invoke(object, new DateTime());
					}
					else if (method.getParameterTypes()[0].isAssignableFrom(String.class)) {
						method.invoke(object, name + RandomStringUtils.randomAscii(RANDOM_LENGTH));
					}
				}
			}
		}
		
	}
}
