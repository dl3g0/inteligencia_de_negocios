USE db_destino;


SET FOREIGN_KEY_CHECKS = 0;


CREATE TABLE `dim_especialidades` (
  `id_especialidad_sk` varchar(250) NOT NULL,
  `id_especialidad` varchar(250) UNIQUE NOT NULL,
  `nombre_especialidad` varchar(255) DEFAULT NULL,
  `fecha_carga` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_especialidad_sk`)
);

CREATE TABLE `dim_pacientes` (
  `id_paciente_sk` varchar(250) NOT NULL,
  `id_paciente` varchar(250) UNIQUE NOT NULL,
  `apellido` varchar(255) DEFAULT NULL,
  `direccion` varchar(255) DEFAULT NULL,
  `fecha_nacimiento` date DEFAULT NULL,
  `genero` varchar(250) DEFAULT NULL,
  `nombre` varchar(255) DEFAULT NULL,
  `telefono` varchar(250) DEFAULT NULL,
  `fecha_carga` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_paciente_sk`),
  UNIQUE KEY `id_paciente` (`id_paciente`)
);

CREATE TABLE `dim_medicos` (
  `id_medico_sk` varchar(250) NOT NULL,
  `id_medico` varchar(250) UNIQUE NOT NULL,
  `id_especialidad` varchar(250) NOT NULL,
  `codigo_empleado` varchar(255) DEFAULT NULL,
  `nombre` varchar(255) DEFAULT NULL,
  `apellido` varchar(255) DEFAULT NULL,
  `genero` varchar(10) DEFAULT NULL,
  `fecha_carga` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_medico_sk`),
  UNIQUE KEY `id_medico` (`id_medico`),
  KEY `FK_dim_medicos_dim_especialidades` (`id_especialidad`),
  CONSTRAINT `FK_dim_medicos_dim_especialidades` FOREIGN KEY (`id_especialidad`) REFERENCES `dim_especialidades` (`id_especialidad`)
);

CREATE TABLE `dim_tiempo` (
  `id_tiempo_sk` varchar(250) NOT NULL,
  `fecha` date DEFAULT NULL,
  `anio` int DEFAULT NULL,
  `mes` int DEFAULT NULL,
  `dia` int DEFAULT NULL,
  `hora` int DEFAULT NULL,
  `minuto` int DEFAULT NULL,
  `segundo` int DEFAULT NULL,
  `nombre_mes` varchar(250) DEFAULT NULL,
  `dia_semana` varchar(250) DEFAULT NULL,
  `fecha_carga` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_tiempo_sk`)
);

CREATE TABLE `citas_hechos` (
  `id_cita` varchar(250) NOT NULL,
  `id_paciente_sk` varchar(250) NOT NULL,
  `id_medico_sk` varchar(250) NOT NULL,
  `id_tiempo_sk` varchar(250) NOT NULL,
  `fecha_hora_cita` datetime DEFAULT NULL,
  `estado_cita` varchar(250) DEFAULT NULL,
  `motivo_cita` varchar(250) DEFAULT NULL,
  `fecha_carga` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_cita`),
  KEY `id_paciente_sk` (`id_paciente_sk`),
  KEY `id_medico_sk` (`id_medico_sk`),
  KEY `id_tiempo_sk` (`id_tiempo_sk`),
  CONSTRAINT `citas_hechos_ibfk_1` FOREIGN KEY (`id_paciente_sk`) REFERENCES `dim_pacientes` (`id_paciente_sk`),
  CONSTRAINT `citas_hechos_ibfk_2` FOREIGN KEY (`id_medico_sk`) REFERENCES `dim_medicos` (`id_medico_sk`),
  CONSTRAINT `citas_hechos_ibfk_3` FOREIGN KEY (`id_tiempo_sk`) REFERENCES `dim_tiempo` (`id_tiempo_sk`)
);

SET FOREIGN_KEY_CHECKS = 1;