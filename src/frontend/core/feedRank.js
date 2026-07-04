export function rankItem(item, user) {
  return (
    0.35 * (item.usageVelocity || 0) +
    0.25 * (item.runtimeEfficiency || 0) +
    0.20 * (item.stackCompatibility || 0) +
    0.10 * (item.novelty || 0) +
    0.10 * (item.successRate || 0)
  );
}

export function buildFeed(items, user) {
  return items
    .map(i => ({...i, score: rankItem(i, user)}))
    .sort((a,b) => b.score - a.score);
}