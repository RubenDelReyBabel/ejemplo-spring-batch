package es.neesis.demospringbatch.processor;

import es.neesis.demospringbatch.dto.OperationDto;
import es.neesis.demospringbatch.model.Operation;
import es.neesis.demospringbatch.model.UserEntity;
import org.springframework.batch.item.ItemProcessor;

public class OperationProcessor implements ItemProcessor<OperationDto, Operation> {

    @Override
    public Operation process(OperationDto user) {
        return Operation.builder()
                .operation(user.getOperation())
                .userEntity(convertToUserEntity(user))
                .build();
    }

    private UserEntity convertToUserEntity(OperationDto user) {
        return UserEntity.builder()
                .username(user.getUsername())
                .password(user.getPassword())
                .email(user.getEmail())
                .build();
    }
}
