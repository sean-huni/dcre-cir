package za.co.fnb.dcre.cir.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.cir.data.model.TxHeaderView;

import java.util.Optional;
import java.util.UUID;

public interface TxHeaderViewRepo extends CrudRepository<TxHeaderView, UUID> {

    Optional<TxHeaderView> findByArrivalId(UUID arrivalId);
}
