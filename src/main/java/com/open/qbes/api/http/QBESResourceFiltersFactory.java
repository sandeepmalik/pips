package com.open.qbes.api.http;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.JobContext;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.core.QueueService;
import com.open.utils.Log;
import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.server.impl.wadl.WadlResource;
import com.sun.jersey.spi.container.*;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class QBESResourceFiltersFactory implements ResourceFilterFactory {

    private static final Log log = Log.getLogger("QBESHTTPContext");

    @Override
    public List<ResourceFilter> create(final AbstractMethod am) {

        if (am.getResource().getResourceClass().isAssignableFrom(WadlResource.class)) {
            return null;
        }

        final String methodName = am.getMethod().getDeclaringClass().getSimpleName() + "." + am.getMethod().getName();

        if (am.getResource().isAnnotationPresent(Path.class)) {
            log.info("Configuring API end point: %s => %s", getOrPost(am.getAnnotations()) + " " + fromAnnotation(am.getResource().getAnnotation(Path.class)) + fromAnnotation(am.getAnnotation(Path.class)), methodName);
        }

        ContextConfiguration contextConfiguration = null;

        List<ResourceFilter> resourceFilters = new ArrayList<>();

        if (am.isAnnotationPresent(ContextConfiguration.class)) {
            contextConfiguration = am.getAnnotation(ContextConfiguration.class);
        }
        if (am.getResource().isAnnotationPresent(ContextConfiguration.class)) {
            contextConfiguration = am.getResource().getAnnotation(ContextConfiguration.class);
        }

        if (contextConfiguration == null) {
            log.warn("%s does not declare an execution strategy", methodName);
            return null;
        }

        final ContextConfiguration strategy = contextConfiguration;
        if (am.isAnnotationPresent(GET.class)) {
            configureGetRequest(methodName, resourceFilters, strategy);
        } else if (am.isAnnotationPresent(POST.class)) {
            configurePostRequest(am, methodName, resourceFilters, strategy);
        }
        return resourceFilters;
    }

    private void configurePostRequest(final AbstractMethod am, final String methodName, List<ResourceFilter> resourceFilters, final ContextConfiguration strategy) {
        resourceFilters.add(new ResourceFilter() {
            @Override
            public ContainerRequestFilter getRequestFilter() {
                return new ContainerRequestFilter() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public ContainerRequest filter(ContainerRequest request) {

                        Map<String, Object> contextData = createCache(strategy.cacheType());
                        if (isJsonMediaType(request.getMediaType())) {
                            HashMap postData = request.getEntity(HashMap.class);
                            if (postData != null) {
                                Map filteredMap = Maps.filterEntries(postData, new Predicate<Map.Entry>() {
                                    @Override
                                    public boolean apply(Map.Entry input) {
                                        return input.getValue() != null;
                                    }
                                });
                                request.setEntity(HashMap.class, HashMap.class, am.getAnnotations(), request.getMediaType(), (MultivaluedMap) request.getRequestHeaders(), postData);
                                contextData.putAll(filteredMap);
                            }
                        }

                        QueueConfig queueConfig = QueueService.getInstance().getQueueConfig(strategy.queueId());
                        try {
                            JobContext<Object> jobContext =
                                    new JobContext<>(queueConfig, contextData, null,
                                            strategy.strategy(),
                                            strategy.jobFactory().newInstance());
                            JobContext.setContext(jobContext);
                            log.debug("Setting context for %s as %s", methodName, JobContext.getContext());
                        } catch (Exception e) {
                            log.fatal("Could not create a context ", e);
                        }
                        return request;
                    }
                };
            }

            @Override
            public ContainerResponseFilter getResponseFilter() {
                return new ContainerResponseFilter() {
                    @Override
                    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
                        log.debug("Removing context %s for %s", JobContext.getContext(), methodName);
                        JobContext.removeContext();
                        return response;
                    }
                };
            }
        });
    }

    private boolean isJsonMediaType(MediaType mediaType) {
        return Constants.APPLICATION_JSON_CHARSET_UTF_8.toLowerCase().equals(mediaType.toString().toLowerCase()) || APPLICATION_JSON_TYPE.equals(mediaType);
    }

    private void configureGetRequest(final String methodName, List<ResourceFilter> resourceFilters, final ContextConfiguration strategy) {
        resourceFilters.add(new ResourceFilter() {
            @Override
            @SuppressWarnings("unchecked")
            public ContainerRequestFilter getRequestFilter() {
                return new ContainerRequestFilter() {
                    @Override
                    public ContainerRequest filter(ContainerRequest request) {
                        QueueConfig queueConfig = QueueService.getInstance().getQueueConfig(strategy.queueId());
                        Map<String, Object> cache = createCache(strategy.cacheType());

                        if (strategy.setGetQueryParamsInCache()) {
                            log.debug("Setting the query params in cache");
                            MultivaluedMap<String, String> queryParams = request.getQueryParameters();
                            for (String param : queryParams.keySet()) {
                                List<String> paramValues = queryParams.get(param);
                                if (paramValues == null || paramValues.size() == 0)
                                    continue;
                                cache.put(param, paramValues);
                            }
                        }

                        String info = "";
                        try {
                            JobContext<String> jobContext =
                                    new JobContext<>(queueConfig, cache, info,
                                            strategy.strategy(),
                                            strategy.jobFactory().newInstance());
                            JobContext.setContext(jobContext);
                            log.debug("Setting context for %s as %s", methodName, JobContext.getContext());
                        } catch (Exception e) {
                            log.fatal("Could not create a context ", e);
                        }
                        return request;
                    }
                };
            }

            @Override
            public ContainerResponseFilter getResponseFilter() {
                return new ContainerResponseFilter() {
                    @Override
                    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
                        log.debug("Removing context %s for %s", JobContext.getContext(), methodName);
                        JobContext.removeContext();
                        return response;
                    }
                };
            }
        });
    }

    private static <M extends Map<String, Object>> M createCache(Class<M> mapClass) {
        try {
            return mapClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Need a default constructor for " + mapClass);
        }
    }

    private static String fromAnnotation(Annotation annotation) {
        if (annotation == null)
            return "";
        if (annotation.annotationType() == GET.class) {
            return "GET";
        } else if (annotation.annotationType() == POST.class) {
            return "POST";
        } else if (annotation.annotationType() == Path.class) {
            return ((Path) annotation).value();
        } else {
            try {
                Method method = annotation.getClass().getMethod("value", String.class);
                if (method != null) {
                    Object valueObj = method.invoke(annotation);
                    if (valueObj != null)
                        return valueObj.toString();
                }
                return annotation.toString();
            } catch (Exception e) {
                return annotation.toString();
            }
        }
    }

    private static String getOrPost(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation == null)
                return "";
            if (annotation.annotationType() == GET.class) {
                return "GET ";
            } else if (annotation.annotationType() == POST.class) {
                return "POST";
            }
        }
        return "";
    }

    private static String fromAnnotations(Annotation[] annotations) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Annotation annotation : annotations) {
            stringBuilder.append(fromAnnotation(annotation)).append(" ");
        }
        return stringBuilder.toString().trim();
    }
}
