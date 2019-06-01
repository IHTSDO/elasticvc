package io.kaicode.elasticvc.api;

import com.google.common.collect.Lists;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import net.jodah.typetools.TypeResolver;
import org.elasticsearch.common.util.set.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
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

	@Value("${elasticvc.save.batch-size:10000}")
	private int saveBatchSize;

	public static final PageRequest LARGE_PAGE = PageRequest.of(0, 10_000);
	public static final int CLAUSE_LIMIT = 65_000;

	private static final Logger logger = LoggerFactory.getLogger(ComponentService.class);

	public static void initialiseIndexAndMappingForPersistentClasses(boolean deleteExisting, ElasticsearchTemplate elasticsearchTemplate, Class<?>... persistentClass) {
		logger.info("Initialising indices");
		Set<Class<?>> classes = Sets.newHashSet(persistentClass);
		classes.add(Branch.class);
		if (deleteExisting) {
			logger.info("Deleting indices");
			for (Class<?> aClass : classes) {
				ElasticsearchPersistentEntity persistentEntity = elasticsearchTemplate.getPersistentEntityFor(aClass);
				logger.info("Deleting index {}", persistentEntity.getIndexName());
				elasticsearchTemplate.deleteIndex(persistentEntity.getIndexName());
			}
		}
		for (Class<?> aClass : classes) {
			ElasticsearchPersistentEntity persistentEntity = elasticsearchTemplate.getPersistentEntityFor(aClass);
			if (!elasticsearchTemplate.indexExists(persistentEntity.getIndexName())) {
				logger.info("Creating index {}", persistentEntity.getIndexName());
				elasticsearchTemplate.createIndex(aClass);
				elasticsearchTemplate.putMapping(aClass);
			}
		}
	}

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
			List<List<C>> batches = Lists.partition(changedOrDeletedComponents, saveBatchSize);
			for (List<C> batch : batches) {
				logger.info("Saving batch of {} {}s", batch.size(), componentClass.getSimpleName());
				final List<String> ids = batch.stream().map(DomainEntity::getId).collect(Collectors.toList());
				versionControlHelper.endOldVersions(commit, idField, componentClass, ids, repository);
				final List<C> changedComponents = batch.stream().filter(d -> !d.isDeleted()).collect(Collectors.toList());
				if (!changedComponents.isEmpty()) {
					versionControlHelper.setEntityMeta(changedComponents, commit);
					repository.saveAll(changedComponents);
				}
			}
		}
		return components.stream().filter(c -> !c.isDeleted()).collect(Collectors.toSet());
	}

	protected String getFetchCount(int size) {
		return "(" + ((size / CLAUSE_LIMIT) + 1) + " fetches)";
	}

	public int getSaveBatchSize() {
		return saveBatchSize;
	}
}
