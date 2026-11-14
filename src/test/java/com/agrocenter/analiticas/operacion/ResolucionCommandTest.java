package com.agrocenter.analiticas.operacion;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResolucionCommandTest {
    @TempDir Path temp;
    @Test void requiereConfirmacionIdentificadorMotivoYArchivoValido() throws Exception {
        String id = "a".repeat(64);
        assertThatThrownBy(() -> ResolucionCommand.Entrada.parse(new String[] {id, "archivo", "motivo"})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ResolucionCommand.Entrada.parse(new String[] {"--confirmo-consumidores-detenidos", "malo", "archivo", "motivo"})).isInstanceOf(IllegalArgumentException.class);
        Path archivo = temp.resolve("corregido.json");
        String[] args = {"--confirmo-consumidores-detenidos", id, archivo.toString(), "revisado"};
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
        Files.writeString(archivo, "");
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
        Files.write(archivo, new byte[1024 * 1024 + 1]);
        assertThatThrownBy(() -> ResolucionCommand.ejecutar(args)).isInstanceOf(IllegalArgumentException.class);
    }
}
