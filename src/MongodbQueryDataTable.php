<?php

namespace Inprohub\DataTables;

use MongoDB\Laravel\Eloquent\Builder as MoloquentBuilder;
use MongoDB\Laravel\Query\Builder;
use Illuminate\Support\Str;
use Yajra\DataTables\DataTableAbstract;
use Yajra\DataTables\QueryDataTable;
use Yajra\DataTables\Utilities\Helper;

class MongodbQueryDataTable extends DataTableAbstract
{
    /**
     * Flag for ordering NULLS LAST option.
     *
     * @var bool
     */
    protected bool $nullsLast = false;

    /**
     * Flag to check if query preparation was already done.
     *
     * @var bool
     */
    protected bool $prepared = false;

    /**
     * Query callback for custom pagination using limit without offset.
     *
     * @var callable|null
     */
    protected $limitCallback = null;

    /**
     * Flag to keep the select bindings.
     *
     * @var bool
     */
    protected bool $keepSelectBindings = false;

    /**
     * Flag to ignore the selects in count query.
     *
     * @var bool
     */
    protected bool $ignoreSelectInCountQuery = false;

    /**
     * Resolve the proper column name be used.
     *
     * @param  string  $column
     * @return string
     */
    protected function resolveRelationColumn(string $column): string
    {
        return $column;
    }

    /**
     * Resolve callback parameter instance.
     *
     * @return QueryBuilder
     */
    protected function resolveCallbackParameter()
    {
        return $this->query;
    }

    /**
     * Get paginated results.
     *
     * @return \Illuminate\Support\Collection<int, array>
     */
    public function results(): \Illuminate\Support\Collection
    {
        return $this->query->get();
    }

    /**
     * Perform column search.
     *
     * @return void
     */
    public function columnSearch(): void
    {
        $columns = $this->request->columns();

        foreach ($columns as $index => $column) {
            $column = $this->getColumnName($index);

            if (is_null($column)) {
                continue;
            }

            if (!$this->request->isColumnSearchable($index) || $this->isBlacklisted($column) && !$this->hasFilterColumn($column)) {
                continue;
            }

            if ($this->hasFilterColumn($column)) {
                $keyword = $this->getColumnSearchKeyword($index, true);
                $this->applyFilterColumn($this->getBaseQueryBuilder(), $column, $keyword);
            } else {
                $column = $this->resolveRelationColumn($column);
                $keyword = $this->getColumnSearchKeyword($index);
                $this->compileColumnSearch($index, $column, $keyword);
            }
        }
    }

    /**
     * Organizes works.
     *
     * @param  bool  $mDataSupport
     * @return \Illuminate\Http\JsonResponse
     *
     * @throws \Exception
     */
    public function make($mDataSupport = true): \Illuminate\Http\JsonResponse
    {
        try {
            $results = $this->prepareQuery()->results();
            $processed = $this->processResults($results, $mDataSupport);
            $data = $this->transform($results, $processed);

            return $this->render($data);
        } catch (\Exception $exception) {
            return $this->errorResponse($exception);
        }
    }

    /**
     * Perform global search for the given keyword.
     *
     * @param  string  $keyword
     * @return void
     */
    protected function globalSearch(string $keyword): void
    {
        $this->query->where(function ($query) use ($keyword) {
            collect($this->request->searchableColumnIndex())
                ->map(function ($index) {
                    return $this->getColumnName($index);
                })
                ->filter()
                ->reject(function ($column) {
                    return $this->isBlacklisted($column) && !$this->hasFilterColumn($column);
                })
                ->each(function ($column) use ($keyword, $query) {
                    if ($this->hasFilterColumn($column)) {
                        $this->applyFilterColumn($query, $column, $keyword, 'or');
                    } else {
                        $this->compileQuerySearch($query, $column, $keyword);
                    }
                });
        });
    }

    /**
     * Can the DataTable engine be created with these parameters.
     *
     * @param mixed $source
     * @return boolean
     */
    public static function canCreate($source)
    {
        return $source instanceof Builder;
    }

    /**
     * @param \Jenssegers\Mongodb\Query\Builder $builder
     */
    public function __construct($builder)
    {
        $this->query = $builder;
        $this->request = app('datatables.request');
        $this->config = app('datatables.config');
        // $this->columns = $builder->query->projections;

        if ($this->config->isDebugging()) {
            $this->getConnection()->enableQueryLog();
        }
    }

    /**
     * @return \Illuminate\Database\Connection
     */
    public function getConnection()
    {
        /** @var Connection $connection */
        $connection = $this->query->getConnection();

        return $connection;
    }

    public function count(): int
    {
        $builder = clone $this->query;

        return $builder->count();
    }

    protected function wrap($column)
    {
        return $column;
    }

    protected function applyFilterColumn($query, $columnName, $keyword, $boolean = 'and')
    {
        $query    = $this->getBaseQueryBuilder($query);
        $callback = $this->columnDef['filter'][$columnName]['method'];

        if ($this->query instanceof MoloquentBuilder) {
            $builder = $this->query->newModelInstance()->newQuery();
        } else {
            $builder = $this->query->newQuery();
        }

        $callback($builder, $keyword);

        $query->addNestedWhereQuery($this->getBaseQueryBuilder($builder), $boolean);
    }

    protected function getBaseQueryBuilder($instance = null)
    {
        if (!$instance) {
            $instance = $this->query;
        }

        if ($instance instanceof MoloquentBuilder) {
            return $instance->getQuery();
        }

        return $instance;
    }

    protected function compileColumnSearch($i, $column, $keyword)
    {
        if ($this->request->isRegex($i)) {
            $this->regexColumnSearch($column, $keyword);
        } else {
            $this->compileQuerySearch($this->query, $column, $keyword, '');
        }
    }

    protected function regexColumnSearch($column, $keyword)
    {
        $this->query->where($column, 'regex', '/' . $keyword . '/' . ($this->config->isCaseInsensitive() ? 'i' : ''));
    }

    protected function castColumn($column)
    {
        return $column;
    }

    protected function compileQuerySearch($query, $column, $keyword, $boolean = 'or')
    {
        $column = $this->castColumn($column);
        $value  = $this->prepareKeyword($keyword);

        if ($this->config->isCaseInsensitive()) {
            $value .= 'i';
        }

        $query->{$boolean . 'Where'}($column, 'regex', $value);
    }

    protected function addTablePrefix($query, $column)
    {
        return $this->wrap($column);
    }

    protected function prepareKeyword($keyword)
    {
        if ($this->config->isWildcard()) {
            $keyword = Helper::wildcardString($keyword, '.*', $this->config->isCaseInsensitive());
        } elseif ($this->config->isCaseInsensitive()) {
            $keyword = Str::lower($keyword);
        }

        if ($this->config->isSmartSearch()) {
            $keyword = "/.*".$keyword.".*/";
        } else {
            $keyword = "/^".$keyword."$/";
        }

        return $keyword;
    }

    /**
     * Not supported
     * Order each given columns versus the given custom sql.
     *
     * @param array  $columns
     * @param string $sql
     * @param array  $bindings
     * @return $this
     */
    public function orderColumns(array $columns, $sql, $bindings = [])
    {
        return $this;
    }

    /**
     * Not supported
     * Override default column ordering.
     *
     * @param string $column
     * @param string $sql
     * @param array  $bindings
     * @return $this
     * @internal string $1 Special variable that returns the requested order direction of the column.
     */
    public function orderColumn($column, $sql, $bindings = [])
    {
        return $this;
    }

    /**
     * Not supported: https://stackoverflow.com/questions/19248806/sort-by-date-with-null-first
     * Set datatables to do ordering with NULLS LAST option.
     *
     * @return $this
     */
    public function orderByNullsLast()
    {
        return $this;
    }

    public function paging(): void
    {
        $limit = (int) ($this->request->input('length') > 0 ? $this->request->input('length') : 10);
        if (is_callable($this->limitCallback)) {
            $this->query->limit($limit);
            call_user_func_array($this->limitCallback, [$this->query]);
        } else {
            $start = (int)$this->request->input('start');
            $this->query->skip($start)->take($limit);
        }
    }

    protected function defaultOrdering(): void
    {
        collect($this->request->orderableColumns())
            ->map(function ($orderable) {
                $orderable['name'] = $this->getColumnName($orderable['column'], true);

                return $orderable;
            })
            ->reject(function ($orderable) {
                return $this->isBlacklisted($orderable['name']) && !$this->hasOrderColumn($orderable['name']);
            })
            ->each(function ($orderable) {
                $column = $this->resolveRelationColumn($orderable['name']);

                if ($this->hasOrderColumn($column)) {
                    $this->applyOrderColumn($column, $orderable);
                } else {
                    $this->query->orderBy($column, $orderable['direction']);
                }
            });
    }

    /**
     * Prepare query by executing count, filter, order and paginate.
     *
     * @return $this
     */
    public function prepareQuery()
    {
        if (!$this->prepared) {
            $this->totalRecords = $this->totalCount();

            $this->filterRecords();
            $this->ordering();
            $this->paginate();
        }

        $this->prepared = true;

        return $this;
    }

    /**
     * Check if column has custom sort handler.
     *
     * @param  string  $column
     * @return bool
     */
    protected function hasOrderColumn(string $column)
    {
        return isset($this->columnDef['order'][$column]);
    }

    /**
     * Check if column has custom filter handler.
     *
     * @param  string  $columnName
     * @return bool
     */
    public function hasFilterColumn(string $columnName)
    {
        return isset($this->columnDef['filter'][$columnName]);
    }

    protected function applyOrderColumn($column, $orderable): void
    {
        $this->query->orderBy($column, $orderable['direction']);
    }
}
