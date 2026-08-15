defmodule TeslaMate.Weather do
  @moduledoc """
  Fetches current weather data from OpenWeather One Call API 4.0.

  API documentation: https://openweathermap.org/api/one-call-4
  """

  use Tesla, only: [:get]

  @version Mix.Project.config()[:version]

  adapter Tesla.Adapter.Finch, name: TeslaMate.HTTP, receive_timeout: 30_000

  plug Tesla.Middleware.BaseUrl, "https://api.openweathermap.org"
  plug Tesla.Middleware.Headers, [{"user-agent", "TeslaMate/#{@version}"}]
  plug Tesla.Middleware.JSON
  plug Tesla.Middleware.Logger, debug: true, log_level: &log_level/1

  @doc """
  Fetches current weather for the given coordinates.

  Returns {:ok, weather_description} where weather_description is the "description"
  field from the weather array (e.g., "阴，多云"), or {:error, reason} on failure.
  """
  def get_current_weather(lat, lon, lang \\ "zh_cn") do
    api_key = System.get_env("OPENWEATHER_API_KEY")
    trimmed_key = if api_key, do: String.trim(api_key), else: ""

    if trimmed_key == "" do
      {:error, :api_key_not_configured}
    else
      params = [
        lat: lat,
        lon: lon,
        lang: lang,
        appid: trimmed_key
      ]

      case get("/data/4.0/onecall/current", query: params) do
        {:ok, %Tesla.Env{status: 200, body: body}} ->
          parse_weather_description(body)

        {:ok, %Tesla.Env{status: 401, body: %{"message" => msg}}} ->
          {:error, {:unauthorized, msg}}

        {:ok, %Tesla.Env{status: status, body: body}} ->
          {:error, {:http_error, status, body}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp parse_weather_description(%{"data" => [%{"weather" => [%{"description" => desc} | _]} | _]}) when is_binary(desc) do
    {:ok, desc}
  end

  defp parse_weather_description(body) do
    Logger.warning("Unexpected OpenWeather response format: #{inspect(body)}")
    {:error, :unexpected_response_format}
  end

  defp log_level(%Tesla.Env{} = env) when env.status >= 400, do: :warning
  defp log_level(%Tesla.Env{}), do: :info
end