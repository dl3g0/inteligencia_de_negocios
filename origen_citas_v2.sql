USE db_origen;

CREATE TABLE `citas` (
  `CitaID` int NOT NULL AUTO_INCREMENT,
  `PacienteID` int NOT NULL,
  `MedicoID` int NOT NULL,
  `FechaCita` date NOT NULL,
  `HoraCita` time NOT NULL,
  `EstadoCita` varchar(50) NOT NULL,
  `MotivoCita` varchar(255) DEFAULT NULL,
  `FechaCreacion` datetime DEFAULT CURRENT_TIMESTAMP,
  `Notas` text,
  PRIMARY KEY (`CitaID`),
  KEY `PacienteID` (`PacienteID`),
  KEY `MedicoID` (`MedicoID`),
  CONSTRAINT `citas_ibfk_1` FOREIGN KEY (`PacienteID`) REFERENCES `pacientes` (`PacienteID`),
  CONSTRAINT `citas_ibfk_2` FOREIGN KEY (`MedicoID`) REFERENCES `medicos` (`MedicoID`)
);


CREATE TABLE `especialidades` (
  `EspecialidadID` int NOT NULL AUTO_INCREMENT,
  `NombreEspecialidad` varchar(255) NOT NULL,
  `Descripcion` text,
  PRIMARY KEY (`EspecialidadID`)
);

CREATE TABLE `medicos` (
  `MedicoID` int NOT NULL AUTO_INCREMENT,
  `Nombre` varchar(255) NOT NULL,
  `Apellido` varchar(255) NOT NULL,
  `CodigoEmpleado` varchar(50) DEFAULT NULL,
  `Genero` varchar(10) DEFAULT NULL,
  `EspecialidadID` int DEFAULT NULL,
  `TelefonoContacto` varchar(20) DEFAULT NULL,
  `Email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`MedicoID`),
  UNIQUE KEY `CodigoEmpleado` (`CodigoEmpleado`),
  KEY `EspecialidadID` (`EspecialidadID`),
  CONSTRAINT `medicos_ibfk_1` FOREIGN KEY (`EspecialidadID`) REFERENCES `especialidades` (`EspecialidadID`)
);

CREATE TABLE `pacientes` (
  `PacienteID` int NOT NULL AUTO_INCREMENT,
  `Nombre` varchar(255) NOT NULL,
  `Apellido` varchar(255) NOT NULL,
  `FechaNacimiento` date DEFAULT NULL,
  `Genero` varchar(10) DEFAULT NULL,
  `Direccion` varchar(255) DEFAULT NULL,
  `Telefono` varchar(20) DEFAULT NULL,
  `Email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`PacienteID`)
);
