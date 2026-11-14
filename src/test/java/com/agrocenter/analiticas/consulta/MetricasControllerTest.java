package com.agrocenter.analiticas.consulta;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.agrocenter.analiticas.security.SeguridadConfig;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(MetricasController.class)
@Import(SeguridadConfig.class)
class MetricasControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean MetricasRepository repository;
    @MockitoBean JwtDecoder decoder;

    @Test void soloAdminConsultaYNoPuedeModificar() throws Exception {
        mvc.perform(get("/api/analiticas/ventas")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/analiticas/ventas").with(jwt().authorities(new SimpleGrantedAuthority("ROLE_CLIENTE"))))
                .andExpect(status().isForbidden());
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        var dia = new MetricasRepository.VentaDiaria(LocalDate.parse("2026-11-12"), 1, new BigDecimal("2.50"));
        when(repository.ventas(any(), any(), eq(0), eq(1))).thenReturn(List.of(dia, dia));
        mvc.perform(get("/api/analiticas/ventas").param("desde", "2026-11-12").param("hasta", "2026-11-14")
                .param("tamano", "1").with(admin)).andExpect(status().isOk())
                .andExpect(jsonPath("$.elementos[0].ventas").value(1)).andExpect(jsonPath("$.hayMas").value(true));
        mvc.perform(post("/api/analiticas/ventas").with(admin)).andExpect(status().isForbidden());
        mvc.perform(delete("/api/analiticas/ventas").with(admin)).andExpect(status().isForbidden());
    }

    @Test void validaRangoYPaginacion() throws Exception {
        var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN"));
        mvc.perform(get("/api/analiticas/ventas").param("desde", "2026-11-13").param("hasta", "2026-11-12").with(admin))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/analiticas/ventas").param("tamano", "101").with(admin)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/analiticas/ventas").param("desde", "fecha-invalida").with(admin)).andExpect(status().isBadRequest());
        verifyNoInteractions(repository);
    }

    @Test void grupoCognitoAdminSeAceptaPeroCustomRoleNo() throws Exception {
        Jwt admin = Jwt.withTokenValue("grupo").header("alg", "RS256").claim("sub", "u").claim("cognito:groups", List.of("ADMIN")).build();
        Jwt custom = Jwt.withTokenValue("custom").header("alg", "RS256").claim("sub", "u").claim("custom:role", "ADMIN").build();
        when(decoder.decode("grupo")).thenReturn(admin);
        when(decoder.decode("custom")).thenReturn(custom);
        when(repository.alertas(0, 25)).thenReturn(List.of());
        mvc.perform(get("/api/analiticas/alertas-stock").header("Authorization", "Bearer grupo")).andExpect(status().isOk());
        mvc.perform(get("/api/analiticas/alertas-stock").header("Authorization", "Bearer custom")).andExpect(status().isForbidden());
    }
}
