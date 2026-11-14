package com.agrocenter.analiticas.eventos;

public class ColisionEventoException extends IllegalStateException {
    public ColisionEventoException() { super("eventId reutilizado con contenido diferente"); }
}
