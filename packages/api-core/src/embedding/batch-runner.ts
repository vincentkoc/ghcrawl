import { ACTIVE_EMBED_DIMENSIONS, EMBED_CONTEXT_RETRY_ATTEMPTS } from '../service-constants.js';
import type { ActiveVectorTask } from '../service-types.js';
import type { AiProvider } from '../openai/provider.js';
import { isEmbeddingContextError, parseEmbeddingContextError, shrinkEmbeddingTask } from './retry.js';

export async function embedBatchWithRecovery(params: {
  ai: AiProvider;
  embedModel: string;
  batch: ActiveVectorTask[];
  onProgress?: (message: string) => void;
}): Promise<Array<{ task: ActiveVectorTask; embedding: number[] }>> {
  try {
    const embeddings = await params.ai.embedTexts({
      model: params.embedModel,
      texts: params.batch.map((task) => task.text),
      dimensions: ACTIVE_EMBED_DIMENSIONS,
    });
    return params.batch.map((task, index) => ({ task, embedding: embeddings[index] }));
  } catch (error) {
    if (!isEmbeddingContextError(error) || params.batch.length === 1) {
      if (params.batch.length === 1 && isEmbeddingContextError(error)) {
        const recovered = await embedSingleTaskWithRecovery({
          ai: params.ai,
          embedModel: params.embedModel,
          task: params.batch[0],
          onProgress: params.onProgress,
        });
        return [recovered];
      }
      throw error;
    }

    params.onProgress?.(`[embed] batch context error; isolating ${params.batch.length} item(s) to find oversized input(s)`);

    const recovered: Array<{ task: ActiveVectorTask; embedding: number[] }> = [];
    for (const task of params.batch) {
      recovered.push(
        await embedSingleTaskWithRecovery({
          ai: params.ai,
          embedModel: params.embedModel,
          task,
          onProgress: params.onProgress,
        }),
      );
    }
    return recovered;
  }
}

async function embedSingleTaskWithRecovery(params: {
  ai: AiProvider;
  embedModel: string;
  task: ActiveVectorTask;
  onProgress?: (message: string) => void;
}): Promise<{ task: ActiveVectorTask; embedding: number[] }> {
  let current = params.task;

  for (let attempt = 0; attempt < EMBED_CONTEXT_RETRY_ATTEMPTS; attempt += 1) {
    try {
      const [embedding] = await params.ai.embedTexts({
        model: params.embedModel,
        texts: [current.text],
        dimensions: ACTIVE_EMBED_DIMENSIONS,
      });
      return { task: current, embedding };
    } catch (error) {
      const context = parseEmbeddingContextError(error);
      if (!context) {
        throw error;
      }

      const next = shrinkEmbeddingTask(current, { embedModel: params.embedModel, context });
      if (!next || next.text === current.text) {
        throw error;
      }
      params.onProgress?.(
        `[embed] shortened #${current.threadNumber}:${current.basis} after context error est_tokens=${current.estimatedTokens}->${next.estimatedTokens}`,
      );
      current = next;
    }
  }

  throw new Error(`Unable to shrink embedding input for #${params.task.threadNumber}:${params.task.basis} below model limits`);
}
