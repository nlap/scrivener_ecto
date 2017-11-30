defimpl Scrivener.Paginater, for: Ecto.Query do
  import Ecto.Query

  alias Scrivener.{Config, Page}

  @moduledoc false

  @spec paginate(Ecto.Query.t, Scrivener.Config.t) :: Scrivener.Page.t
  def paginate(query, %Config{page_size: page_size, page_number: page_number, module: repo, caller: caller, options: options}) do
    total_entries = Keyword.get_lazy(options, :total_entries, fn -> total_entries(query, repo, caller) end)
    total_pages = total_pages(total_entries, page_size)
    page_number = min(total_pages, page_number)

    %Page{
      page_size: page_size,
      page_number: page_number,
      entries: entries(query, repo, page_number, page_size, caller),
      total_entries: total_entries,
      total_pages: total_pages
    }
  end

  defp entries(query, repo, page_number, page_size, caller) do
    offset = page_size * (page_number - 1)

    query
    |> limit(^page_size)
    |> offset(^offset)
    |> repo.all(caller: caller)
  end

  defp total_entries(query, repo, caller) do

    if is_map(query.from) && Map.has_key?(query.from, :__struct__) do
      total_entries =
        query.from.query
        |> exclude(:preload)
        |> exclude(:order_by)
        |> prepare_select
        |> count
        |> repo.one(caller: caller)

      total_entries || 0
    else
      primary_key =
        query.from
        |> elem(1)
        |> apply(:__schema__, [:primary_key])
        |> hd

      query
      |> exclude(:order_by)
      |> exclude(:preload)
      |> exclude(:select)
      |> exclude(:group_by)
      |> select([m], count(field(m, ^primary_key), :distinct))
      |> repo.one!
    end

  end

  defp prepare_select(
    %{
      group_bys: [
        %Ecto.Query.QueryExpr{
          expr: [
            {{:., [], [{:&, [], [source_index]}, field]}, [], []} | _
          ]
        } | _
      ]
    } = query
  ) do
    query
    |> exclude(:select)
    |> select([x: source_index], struct(x, ^[field]))
  end
  defp prepare_select(query) do
    query
    |> exclude(:select)
  end

  defp count(query) do
    query
    |> subquery
    |> select(count("*"))
  end

  defp total_pages(0, _), do: 1

  defp total_pages(total_entries, page_size) do
    (total_entries / page_size) |> Float.ceil |> round
  end
end
