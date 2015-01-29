namespace MongoMigrations
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using MongoDB.Bson.Serialization;
	using MongoDB.Driver;
	using System.Threading.Tasks;

	public class MigrationRunner
	{
		static MigrationRunner()
		{
			Init();
		}

		public static void Init()
		{
			BsonSerializer.RegisterSerializer(typeof (MigrationVersion), new MigrationVersionSerializer());
		}

		public MigrationRunner(string mongoServerLocation)
			: this(new MongoUrl(mongoServerLocation))
		{
		}

		private MigrationRunner(MongoUrl url)
		{
			Url = url;
			Client = new MongoClient(url);
			Database = Client.GetDatabase(url.DatabaseName);
			DatabaseStatus = new DatabaseMigrationStatus(this);
			MigrationLocator = new MigrationLocator();
		}

		public MongoUrl Url { get; private set; }
		public IMongoClient Client { get; private set; }
		public IMongoDatabase Database { get; private set; }
		public MigrationLocator MigrationLocator { get; set; }
		public DatabaseMigrationStatus DatabaseStatus { get; set; }

		public virtual void UpdateToLatest()
		{
			Console.WriteLine(WhatWeAreUpdating() + " to latest...");
			UpdateTo(MigrationLocator.LatestVersion());
		}

		private string WhatWeAreUpdating()
		{
			return string.Format("Updating server(s) \"{0}\" for database \"{1}\"", ServerAddresses(), Url.DatabaseName);
		}

		private string ServerAddresses()
		{
			return string.Join(",", Url.Servers.Select(s => s.Host.ToString()));
		}

		protected virtual void ApplyMigrations(IEnumerable<Migration> migrations)
		{
			foreach (var migration in migrations)
			{
				ApplyMigration(migration).Wait();
			}
		}

		protected virtual async Task ApplyMigration(Migration migration)
		{
			Console.WriteLine(new {Message = "Applying migration", migration.Version, migration.Description, DatabaseName = Url.DatabaseName});

			var appliedMigration = DatabaseStatus.StartMigration(migration);
			migration.Database = Database;
			try
			{
				await migration.RunUpdates();
			}
			catch (Exception exception)
			{
				OnMigrationException(migration, exception);
			}
			DatabaseStatus.CompleteMigration(appliedMigration);
		}

		protected virtual void OnMigrationException(Migration migration, Exception exception)
		{
			var message = new
				{
					Message = "Migration failed to be applied: " + exception.Message,
					migration.Version,
					Name = migration.GetType(),
					migration.Description,
					DatabaseName = Url.DatabaseName
				};
			Console.WriteLine(message);
			throw new MigrationException(message.ToString(), exception);
		}

		public virtual void UpdateTo(MigrationVersion updateToVersion)
		{
			var currentVersion = DatabaseStatus.GetLastAppliedMigration();
			Console.WriteLine(new {Message = WhatWeAreUpdating(), currentVersion, updateToVersion, DatabaseName = Url.DatabaseName});

			var migrations = MigrationLocator.GetMigrationsAfter(currentVersion)
											 .Where(m => m.Version <= updateToVersion);

			ApplyMigrations(migrations);
		}
	}
}