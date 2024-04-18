package es.neesis.demospringbatch.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OperationDto {
    private String operation;
    private String username;
    private String password;
    private String email;
}
