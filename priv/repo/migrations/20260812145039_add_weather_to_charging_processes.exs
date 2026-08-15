defmodule TeslaMate.Repo.Migrations.AddWeatherToChargingProcesses do
  use Ecto.Migration

  def change do
    alter table(:charging_processes) do
      add :weather, :string
    end
  end
end