# inference/preprocessing/preprocess_postgres_plans.py

import numpy as np
from inference.preprocessing.preprocessor import QueryPlanPreprocessor
import inference.preprocessing.preprocess_presto_plans as presto_prep
from inference.preprocessing.preprocess_presto_plans import (
    TreeBuilder,
    _get_plan_stats,
    _get_all_relations,
    CPU_COST,
    ROWS,
)

# PostgreSQL EXPLAIN(JSON) 필드 이름
CHILDREN_FIELD   = 'Plans'
NODE_TYPE_FIELD  = 'Node Type'
PLAN_ROWS_FIELD  = 'Plan Rows'
TOTAL_COST_FIELD = 'Total Cost'
PREPROCESSED     = '_ast_preprocessed'

# Presto 헬퍼에 덮어쓰기 (ESTIMATES는 건드리지 마세요!)
presto_prep.CHILDREN  = CHILDREN_FIELD
presto_prep.NODE_TYPE = NODE_TYPE_FIELD
presto_prep.COST      = TOTAL_COST_FIELD

# 인코딩 대상 노드 타입
ENCODED_TYPES = [
    'Seq Scan', 'Index Scan', 'Aggregate',
    'Hash Join','Merge Join','Nested Loop',
    'Sort','Filter','Limit'
]
presto_prep.ENCODED_TYPES    = ENCODED_TYPES
presto_prep.BINARY_OPERATORS = ['Hash Join','Merge Join','Nested Loop']
presto_prep.UNARY_OPERATORS  = [t for t in ENCODED_TYPES if t not in presto_prep.BINARY_OPERATORS]

# --- TreeBuilder.plan_to_feature_tree 패치 ---
_original_pt = TreeBuilder.plan_to_feature_tree

def _patched_plan_to_feature_tree(self, node):
    children  = node.get(CHILDREN_FIELD, [])
    node_type = node.get(NODE_TYPE_FIELD)

    # 1) 투명 처리할 노드
    TRANSPARENT = {'Limit','Sort','Result','SubPlan'}
    if node_type in TRANSPARENT:
        return self.plan_to_feature_tree(children[0]) if children else self._TreeBuilder__featurize_null_operator()

    # 2) encoding 대상이 아니면 자식 언랩 or null
    if node_type not in ENCODED_TYPES:
        return self.plan_to_feature_tree(children[0]) if len(children)==1 else self._TreeBuilder__featurize_null_operator()

    # 3) binary
    from inference.preprocessing.preprocess_presto_plans import is_binary_operator, is_unary_operator, is_leaf_operator
    if is_binary_operator(node):
        left  = self.plan_to_feature_tree(children[0]) if len(children)>0 else self._TreeBuilder__featurize_null_operator()
        right = self.plan_to_feature_tree(children[1]) if len(children)>1 else self._TreeBuilder__featurize_null_operator()
        return self._TreeBuilder__featurize_binary_operator(node), left, right

    # 4) leaf
    if is_leaf_operator(node) or not children:
        return self._TreeBuilder__featurize_unary_operator(node)

    # 5) unary
    if is_unary_operator(node):
        child = self.plan_to_feature_tree(children[0]) if children else self._TreeBuilder__featurize_null_operator()
        return self._TreeBuilder__featurize_unary_operator(node), child, self._TreeBuilder__featurize_null_operator()

    # 그 외 null
    return self._TreeBuilder__featurize_null_operator()

TreeBuilder.plan_to_feature_tree = _patched_plan_to_feature_tree

# --- StatExtractor (변경 없음) ---
class StatExtractor:
    def __init__(self, fields, mins, maxs):
        self.fields = fields; self.mins = mins; self.maxs = maxs
    def __call__(self, node):
        res = []
        for f, lo, hi in zip(self.fields, self.mins, self.maxs):
            v = node.get(f,0.0) or 0.0
            if v==0.0: res += [0,0]
            else:
                n = (np.log(v+1)-lo)/(hi-lo) if hi!=lo else 0.0
                res += [1, n]
        return res
    def get_null_stats(self):
        return [0.0] * (2*len(self.fields))

# --- 최종 Preprocessor ---
class PostgresPlanPreprocessor(QueryPlanPreprocessor):
    def __init__(self):
        super().__init__()
        self._tree_builder = None

    def fit(self, trees):
        for t in trees:
            self.preprocess(t)
        rels  = _get_all_relations(trees)
        stats = _get_plan_stats(trees)
        self._tree_builder = TreeBuilder(stats, rels)

    def transform(self, trees):
        for t in trees:
            self.preprocess(t)
        return [self._tree_builder.plan_to_feature_tree(t) for t in trees]

    def preprocess(self, plan):
        if PREPROCESSED in plan:
            return
        # PostgreSQL → presto_estimates 포맷 래핑
        plan['estimates'] = {
            CPU_COST: plan.get(TOTAL_COST_FIELD, 0.0),
            ROWS:     plan.get(PLAN_ROWS_FIELD,   0.0)
        }
        plan[PREPROCESSED] = True
        for c in plan.get(CHILDREN_FIELD, []):
            self.preprocess(c)