defmodule TeslaMate.Repo.Migrations.AddWeatherToDrives do
  use Ecto.Migration

  def change do
    alter table(:drives) do
      add :weather, :string
    end
  end
end