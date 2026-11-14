package com.agrocenter.analiticas.consulta;

import java.time.Instant;

/** Solo metadatos de diagnóstico; nunca contiene el payload original. */
public record CuarentenaMetadata(String id, String stream, String shard, String sequenceNumber,
                                 long subSequenceNumber, String tipoError, Instant createdAt) { }
