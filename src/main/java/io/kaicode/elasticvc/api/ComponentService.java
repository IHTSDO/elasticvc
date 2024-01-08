package io.kaicode.elasticvc.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import net.jodah.typetools.TypeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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


	public static void initialiseIndexAndMappingForPersistentClasses(boolean deleteExisting, ElasticsearchOperations elasticsearchOperations, Map<String, Object> settings, Class<?>... persistentClass) {
		Set<Class<?>> classes = Sets.newHashSet(persistentClass);
		classes.add(Branch.class);
		logger.info("Initialising {} indices", classes.size());
		if (deleteExisting) {
			logger.info("Deleting indices");
			for (Class<?> aClass : classes) {
				IndexCoordinates index = elasticsearchOperations.getIndexCoordinatesFor(aClass);
				logger.info("Deleting index {}", index.getIndexName());
				elasticsearchOperations.indexOps(index).delete();
			}
		}
		for (Class<?> aClass : classes) {
			IndexCoordinates index = elasticsearchOperations.getIndexCoordinatesFor(aClass);
			IndexOperations indexOperations = elasticsearchOperations.indexOps(index);
			if (!indexOperations.exists()) {
				logger.info("Creating index {}", index.getIndexName());
				if (settings == null || settings.isEmpty()) {
					indexOperations.create(indexOperations.createSettings(aClass));
				} else {
					indexOperations.create(settings);
				}
				indexOperations.putMapping(indexOperations.createMapping(aClass));
			}
		}
	}

	public static void initialiseIndexAndMappingForPersistentClasses(boolean deleteExisting, ElasticsearchOperations elasticsearchOperations, Class<?>... persistentClass) {
		initialiseIndexAndMappingForPersistentClasses(deleteExisting, elasticsearchOperations, null, persistentClass);
	}

	@SuppressWarnings("unchecked")
	/*
	  Saves components within commit.
	  @return The saved components with updated metadata not including those which were deleted.
	 */
	protected <C extends DomainEntity> Iterable<C> doSaveBatchComponents(Collection<C> components, Commit commit, String idField, ElasticsearchRepository<C, String> repository) {
		final Class<?>[] classes = TypeResolver.resolveRawArguments(ElasticsearchRepository.class, repository.getClass());
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
