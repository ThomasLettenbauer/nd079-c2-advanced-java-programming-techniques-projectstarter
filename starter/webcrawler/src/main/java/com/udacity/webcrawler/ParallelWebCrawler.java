package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.json.CrawlResultWriter;
import com.udacity.webcrawler.json.CrawlerConfiguration;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;


  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls,
      @TargetParallelism int threadCount) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactory =parserFactory;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }



  @Override
  public CrawlResult crawl(List<String> startingUrls) throws IOException {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<String>());
    CrawlResult result;

    for (String url : startingUrls) {
      CrawlTask task = new CrawlTask(url, deadline, maxDepth, counts, visitedUrls);
      pool.invoke(task);
    }

    if (counts.isEmpty()) {
      result = new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    } else {
    result = new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build(); }

    CrawlResultWriter writer = new CrawlResultWriter(result);
    PrintWriter printWriter = new PrintWriter(System.out);

    /* Only for testing
    try {
      writer.write(printWriter);
    } catch (IOException e) {System.out.print(e.getLocalizedMessage());}
    */

    return result;

  }

  class CrawlTask extends RecursiveAction {

    String url;
    Instant deadline;
    int maxDepth;
    Map<String, Integer> counts;
    Set<String> visitedUrls;

    CrawlTask (String url, Instant deadline, int maxDepth, Map<String, Integer> counts, Set<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls; }

      @Override
      protected void compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
          return;
        }

        for (Pattern pattern : ignoredUrls) {
          if (pattern.matcher(url).matches()) {
            return;
          }
        }
        if (visitedUrls.contains(url)) {
          return;
        }
        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
          if (counts.containsKey(e.getKey())) {
            counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
          } else {
            counts.put(e.getKey(), e.getValue());
          }
        }
        for (String link : result.getLinks()) {
          pool.invoke(new CrawlTask(link, deadline, maxDepth - 1, counts, visitedUrls));
        }
      }
    }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}

