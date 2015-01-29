namespace MongoMigrations
{
    using MongoDB.Driver;
    using System.Threading.Tasks;

    public abstract class Migration
    {
        public MigrationVersion Version { get; protected set; }
        public string Description { get; protected set; }

        protected Migration(MigrationVersion version)
        {
            Version = version;
        }

        public IMongoDatabase Database { get; set; }

        public virtual Task UpdateAsync()
        {
            return Task.Run(() => { });
        }

        public virtual void Update()
        {
        }

        internal async Task RunUpdates()
        {
            await UpdateAsync();
            Update();
        }
    }
}