package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import net.jodah.typetools.TypeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ComponentService {

	@Autowired
	private VersionControlHelper versionControlHelper;

	public static final PageRequest LARGE_PAGE = new PageRequest(0, 10000);
	public static final int CLAUSE_LIMIT = 800;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@SuppressWarnings("unchecked")
	/**
	 * Saves components within commit.
	 * @return The saved components with updated metadata not including those which were deleted.
	 */
	protected <C extends DomainEntity> Iterable<C> doSaveBatchComponents(Collection<C> components, Commit commit, String idField, ElasticsearchCrudRepository<C, String> repository) {
		final Class<?>[] classes = TypeResolver.resolveRawArguments(ElasticsearchCrudRepository.class, repository.getClass());
		Class<C> componentClass = (Class<C>) classes[0];
		commit.addDomainEntityClass(componentClass);
		final List<C> changedOrDeletedComponents = components.stream().filter(component -> component.isChanged() || component.isDeleted()).collect(Collectors.toList());
		final Set<String> deletedComponentIds = changedOrDeletedComponents.stream().filter(DomainEntity::isDeleted).map(DomainEntity::getId).collect(Collectors.toSet());
		commit.addVersionsDeleted(deletedComponentIds);
		if (!changedOrDeletedComponents.isEmpty()) {
			logger.info("Saving batch of {} {}s", changedOrDeletedComponents.size(), componentClass.getSimpleName());
			final List<String> ids = changedOrDeletedComponents.stream().map(DomainEntity::getId).collect(Collectors.toList());
			versionControlHelper.endOldVersions(commit, idField, componentClass, ids, repository);
			final List<C> changedComponents = changedOrDeletedComponents.stream().filter(d -> !d.isDeleted()).collect(Collectors.toList());
			if (!changedComponents.isEmpty()) {
				versionControlHelper.setEntityMeta(changedComponents, commit);
				repository.saveAll(changedComponents);
			}
		}
		return components.stream().filter(c -> !c.isDeleted()).collect(Collectors.toSet());
	}

	protected String getFetchCount(int size) {
		return "(" + ((size / CLAUSE_LIMIT) + 1) + " fetches)";
	}

}
