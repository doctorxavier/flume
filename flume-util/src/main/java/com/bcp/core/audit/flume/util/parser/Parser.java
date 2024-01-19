package com.bcp.core.audit.flume.util.parser;

import java.io.InputStream;
import java.io.OutputStream;

import com.bcp.core.audit.flume.util.parser.exception.ParserException;

public interface Parser<E> {

    E unmarshall(InputStream in) throws ParserException;
    
    E unmarshall(String inPath) throws ParserException;
    
    E unmarshallFromString(String marshall) throws ParserException;

    void marshall(E marshall, OutputStream out) throws ParserException;
    
    String toString(E marshall) throws ParserException;

    String toString(E marshall, int indent) throws ParserException;

}
