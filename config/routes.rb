# frozen_string_literal: true

Pgbus::Engine.routes.draw do
  # SSE streaming endpoint for the pgbus_stream_from turbo-rails replacement.
  # Mounted as a bare Rack app so it bypasses the entire Rails middleware
  # stack — see lib/pgbus/web/stream_app.rb for the rationale. Named with
  # `as: :streams` so `Pgbus::Engine.routes.url_helpers.streams_path`
  # resolves to whatever mount point the host app chose for the engine,
  # not a hardcoded `/pgbus/streams`. The helper under
  # app/helpers/pgbus/streams_helper.rb appends `/:signed_name` to that
  # base so the full URL works whether the engine is at /pgbus, /admin,
  # or anywhere else.
  mount Pgbus::Web::StreamApp.new => "/streams", as: :streams if Pgbus.configuration.streams_enabled

  root to: "dashboard#show"

  resources :queues, only: %i[index show destroy], param: :name do
    member do
      post :purge
      post :pause
      post :resume
      post :retry_message
      post :discard_message
    end
  end

  resources :jobs, only: %i[index show] do
    member do
      post :retry
      post :discard
    end
    collection do
      post :retry_all
      post :discard_all
      post :discard_all_enqueued
      post :discard_selected_failed
      post :discard_selected_enqueued
    end
  end

  resources :recurring_tasks, only: %i[index show] do
    member do
      post :toggle
      post :enqueue
    end
  end

  resources :batches, only: %i[index show]
  resources :processes, only: [:index]

  resources :events, only: %i[index show] do
    member do
      post :replay
      post :discard
      post :mark_handled
      post :edit_payload
      post :reroute
    end
    collection do
      post :discard_selected
    end
  end

  resources :dead_letter, only: %i[index show], path: "dlq" do
    member do
      post :retry
      post :discard
    end
    collection do
      post :retry_all
      post :discard_all
      post :discard_selected
    end
  end

  resources :outbox, only: [:index], controller: "outbox"
  resources :locks, only: [:index] do
    member do
      post :discard
    end
    collection do
      post :discard_selected
      post :discard_all
      # Collection routes, not member: a concurrency key is a free-form
      # string that may contain ".", "/" or ":" and does not belong in a
      # path segment. Both take the key as a form param.
      post :release_key
      post :discard_parked
    end
  end
  resource :insights, only: [:show], controller: "insights"

  get :set_locale, to: "locale#update"

  namespace :api do
    get :stats, to: "stats#show"
    get :insights, to: "insights#show"
    get :metrics, to: "metrics#show"
  end

  scope :frontend, controller: :frontends, defaults: { version: Pgbus::VERSION.tr(".", "-") } do
    get "modules/:version/:id", action: :module, as: :frontend_module, constraints: { format: "js" }
    get "static/:version/:id", action: :static, as: :frontend_static
  end
end
