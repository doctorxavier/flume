package com.bcp.core.audit.flume.interceptor.audit.hdfs.dto;

import java.io.Serializable;

public class AuditInfo implements Serializable {

	private static final long serialVersionUID = -1L;

	private String nombre;

	//private String apellido;

	//private String edad;

	//private String fechanacimiento;

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	/*public String getApellido() {
		return apellido;
	}

	public void setApellido(String apellido) {
		this.apellido = apellido;
	}

	public String getEdad() {
		return edad;
	}

	public void setEdad(String edad) {
		this.edad = edad;
	}

	public String getFechanacimiento() {
		return fechanacimiento;
	}

	public void setFechanacimiento(String fechanacimiento) {
		this.fechanacimiento = fechanacimiento;
	}
*/
}
